// nbbo_pipeline.cpp — C++23
// CSV.gz -> msbin (event or clock) -> fast tail winsor -> Parquet per-year (partitioned)
//
// Features:
// - Separate caches: cache/ms_event (no ffill) and cache/ms_clock (ffill).
// - Cache-only mode: runs even if --in is empty/missing (no T7), from cache.
// - Event→Clock fallback: if --clock and ms_clock is empty but ms_event exists,
//   synthesize ms_clock by per-day ffill for gaps <= --max-ffill-gap-ms.
// - Winsor: parallel exact tail selection (tiny heaps). Fast and bias-light for 1e-5 tails.
// - Parquet output: partitioned by year into out/<event|event_winsor|clock|clock_winsor>/SYM_YYYY.parquet.
//   Cross-year msbins (e.g. 202401_11 has 2023+2024) are split by each row’s timestamp year.
//
// Build: cmake -S . -B build -G Ninja && cmake --build build -j

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>
#include <zlib.h>

#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <cerrno>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <vector>
#include <cctype>
#include <queue>
#include <stdexcept>
#include "nbbo/arrow_utils.hpp"
#include "nbbo/time_utils.hpp"
#include "nbbo/schema.hpp"

using std::string; using std::string_view;
namespace fs = std::filesystem;

static inline void ARROW_OK(const arrow::Status& st) {
    if (!st.ok()) throw std::runtime_error(st.ToString());
}

/************** Settings *************************/
struct Settings {
    fs::path in_dir, cache_dir, out_parquet, report_path;

    bool event_grid   = true;      // default to event grid
    bool clock_grid   = false;     // enable with --clock
    bool ffill        = false;     // only used when clock_grid
    int  max_ffill_gap_ms = 250;   // cap for clock-grid fills

    bool winsorize    = false;
    bool winsor_clip  = false;     // else drop
    double q_lo = 1e-5, q_hi = 1.0 - 1e-5;

    int rth_start_h=9, rth_start_m=30, rth_end_h=16, rth_end_m=0;
    std::set<char> venues = {'P','T','Q','Z','Y','J','K'};
    int stale_ms = 80;

    uint64_t log_every_in  = 5'000'000;
    uint64_t log_every_out = 1'000'000;

    std::string sym_root = "SPY";
    int year_lo = 0, year_hi = 0;

    int workers = (int)std::max(1u, std::thread::hardware_concurrency());
};

static void usage(){
    std::cerr <<
    "nbbo_pipeline --in DIR --cache DIR --out OUT_PATH --report FILE.txt\n"
    "  [--clock] [--event] [--ffill] [--no-ffill] [--max-ffill-gap-ms N]\n"
    "  [--winsor] [--winsor-clip|--winsor-drop] [--winsor-quantiles a,b]\n"
    "  [--rth HH:MM:SS-HH:MM:SS] [--ex VENUES] [--stale-ms N]\n"
    "  [--log-every-in N] [--log-every-out N]\n"
    "  [--sym-root SYM] [--years YYYY:YYYY] [--workers N]\n"
    "Note: OUT_PATH may be a directory or a .parquet path; for partitioned output we use the directory.\n";
}

static bool parse_time_hms(string_view s, int& h,int& m,int& sec){
    if(s.size()<8) return false;
    auto to2=[&](int off){ return std::stoi(string(s.substr(off,2))); };
    h=to2(0); m=to2(3); sec=to2(6); return true;
}

/************** Types ***************/
struct Quote {
    uint64_t ts;     // yyyymmddHHMMSSmmm
    float bid, ask;
    int32_t bidSize, askSize;
    char ex;
};
struct Row {
    uint64_t ts;
    float mid, logret, bidSize, askSize, spread, bid, ask;
};

/************** Glitches *************/
struct GlitchCounts {
    std::map<string,uint64_t> total;
    std::map<string,std::map<int,uint64_t>> by_hour;
    void bump(const string& cat,int hour){ ++total[cat]; ++by_hour[cat][hour]; }
    void merge(const GlitchCounts& o){
        for(auto& [k,v]: o.total) total[k]+=v;
        for(auto& [k,hm]: o.by_hour) for(auto& [h,c]: hm) by_hour[k][h]+=c;
    }
    void write_report(const fs::path& p){
        std::ofstream r(p);
        r<<"NBBO pipeline glitch report\n\nTotals:\n";
        for(auto& [k,v]: total) r<<std::setw(22)<<std::left<<k<<" : "<<v<<"\n";
        r<<"\nBy hour (RTH):\n";
        for(auto& [k,hmap]: by_hour){
            r<<"\n["<<k<<"]\n";
            for(int h=9; h<=15; ++h){
                uint64_t c = hmap.count(h)?hmap.at(h):0;
                r<<"  "<<h<<":00 - "<<c<<"\n";
            }
        }
    }
};

/************** Helpers **************/
static inline bool in_rth(int h,int m,int s,const Settings& S){
    if(h < S.rth_start_h || h > S.rth_end_h-1) return false;
    if(h==S.rth_start_h && m < S.rth_start_m)  return false;
    if(h==S.rth_end_h) return false;
    (void)s; return true;
}
static inline bool is_good_ex(char ex,const Settings& S){ return S.venues.count(ex)>0; }

/************** Fast gz line reader *****/
struct GzLine {
    gzFile f{nullptr};
    std::string buf;
    explicit GzLine(const fs::path& p){
        f = gzopen(p.string().c_str(), "rb");
        if (f) gzbuffer(f, 1<<20);
        buf.resize(1<<16);
    }
    ~GzLine(){ if(f) gzclose(f); }
    bool good() const { return f!=nullptr; }
    bool getline(std::string& out){
        out.clear(); if(!f) return false;
        for(;;){
            char* r = gzgets(f, buf.data(), (int)buf.size());
            if(!r) return !out.empty();
            size_t n = std::strlen(r);
            if(n && r[n-1]=='\n'){ out.append(r,n-1); return true; }
            out.append(r,n);
        }
    }
};

/************** NBBO per-ms bucket ******/
struct NBBOBucket {
    uint64_t ms=0;
    float bestBid=0.f, bestAsk=std::numeric_limits<float>::infinity();
    int32_t bidSz=0, askSz=0;
    bool any=false;
    void reset(uint64_t t){ ms=t; bestBid=0.f; bestAsk=std::numeric_limits<float>::infinity(); bidSz=askSz=0; any=false; }
    void upd(const Quote& q, GlitchCounts& G, int h){
        if(q.bid<=0 || q.ask<=0){ G.bump("nonpos_price",h); return; }
        if(q.ask <= q.bid){ G.bump("locked_crossed",h); return; }
        if(q.bid > bestBid){ bestBid=q.bid; bidSz=q.bidSize; any=true; }
        if(q.ask < bestAsk){ bestAsk=q.ask; askSz=q.askSize; any=true; }
    }
    bool out(Row& r, float prev_mid, bool set_lr, float& new_mid){
        if(!any) return false;
        r.ts=ms; r.bid=bestBid; r.ask=bestAsk;
        r.bidSize=(float)bidSz; r.askSize=(float)askSz;
        r.spread=bestAsk-bestBid; r.mid=0.5f*(bestBid+bestAsk);
        if(set_lr && prev_mid>0.f && r.mid>0.f) r.logret = std::log(r.mid/prev_mid);
        else r.logret = std::numeric_limits<float>::quiet_NaN();
        new_mid=r.mid; return true;
    }
};

/************** msbin I/O **************/
#pragma pack(push,1)
struct MsBinRow {
    uint64_t ts;
    float mid, logret, bidSize, askSize, spread, bid, ask;
};
#pragma pack(pop)

/************** Pipeline ***************/
struct Pipeline {
    Settings S;
    std::mutex gl_mu;
    GlitchCounts gl_total;

    std::atomic<uint64_t> p_in{0}, p_out{0};

    fs::path cache_subdir() const {
        return S.cache_dir / (S.clock_grid ? "ms_clock" : "ms_event");
    }

    static bool starts_with(const std::string& s, const std::string& p){
        return s.size()>=p.size() && std::equal(p.begin(), p.end(), s.begin());
    }

    static int extract_year(const std::string& fname, const std::string& sym) {
        size_t pos = sym.size();
        if (fname.size() < pos + 4) return -1;
        if (!std::isdigit((unsigned char)fname[pos]) ||
            !std::isdigit((unsigned char)fname[pos+1]) ||
            !std::isdigit((unsigned char)fname[pos+2]) ||
            !std::isdigit((unsigned char)fname[pos+3])) return -1;
        return std::stoi(fname.substr(pos, 4));
    }

    static void sort_chronologically(const Settings& S, std::vector<fs::path>& files){
        std::sort(files.begin(), files.end(),
                  [&](const fs::path& a, const fs::path& b){
                      return extract_year(a.filename().string(), S.sym_root) <
                             extract_year(b.filename().string(), S.sym_root);
                  });
    }

    fs::path msbin_path_for_csv(const fs::path& csv) const {
        auto name = csv.filename().string();
        auto dot = name.find(".csv.gz");
        auto base = name.substr(0,dot);
        return cache_subdir() / (base + ".msbin");
    }

    // Stage A: CSV.gz -> .msbin (event or clock depending on flags)
    void process_file_to_msbin(const fs::path& csv, const fs::path& msbin){
        GlitchCounts G;
        fs::create_directories(msbin.parent_path());
        std::ofstream bin(msbin, std::ios::binary);
        if(!bin) throw std::runtime_error("open msbin for write failed: " + msbin.string());

        GzLine gz(csv);
        if(!gz.good()) throw std::runtime_error("open gzip failed: " + csv.string());
        string line; gz.getline(line); // header

        NBBOBucket bucket; bucket.reset(0);
        float prev_mid=0.f; uint32_t prev_date=0; bool have_prev=false;
        Row prev_row{}; bool have_prev_row=false;
        uint64_t last_emit=0;

        struct Fields { string f[14]; int n=0; void split(const string& l){
            n=0; int start=0; int L=(int)l.size();
            for(int i=0;i<L;++i){ if(l[i]==','){ f[n++]=string(l.data()+start,i-start); start=i+1; if(n==13) break; } }
            f[n++]=string(l.data()+start, L-start);
        }} fld;

        uint64_t in_local=0, out_local=0;

        auto parse_float = [](std::string_view s, float& out){
            char buf[64]; if (s.size() >= sizeof(buf)) return false;
            std::memcpy(buf, s.data(), s.size()); buf[s.size()] = '\0';
            char* end = nullptr; errno = 0;
            float v = std::strtof(buf, &end);
            if (errno != 0 || end != buf + s.size()) return false;
            out = v; return true;
        };
        auto parse_int32 = [](std::string_view s, int32_t& out){
            const char* b = s.data(); const char* e = b + s.size();
            auto r = std::from_chars(b, e, out);
            return r.ec == std::errc() && r.ptr == e;
        };
        auto parse_u64 = [](std::string_view s, uint64_t& out){
            const char* b = s.data(); const char* e = b + s.size();
            auto r = std::from_chars(b, e, out);
            return r.ec == std::errc() && r.ptr == e;
        };

        while(gz.getline(line)){
            ++in_local;
            if((in_local % S.log_every_in)==0){
                auto tot = p_in.fetch_add(S.log_every_in, std::memory_order_relaxed) + S.log_every_in;
                std::cerr << "[stageA] " << csv.filename().string() << " in=" << tot << "\n";
            }

            fld.split(line);
            if(fld.n<9) continue;

            string_view date=fld.f[0], time=fld.f[1], exs=fld.f[2];
            string_view sbid=fld.f[3], sbs=fld.f[4], sask=fld.f[5], sas=fld.f[6], qc=fld.f[7];

            if(qc.size()!=1 || qc[0]!='R') continue;
            if(exs.empty() || !is_good_ex(exs[0],S)) continue;

            int h=0,m=0,s=0; if(!parse_time_hms(string(time.substr(0,8)),h,m,s)) continue;
            if(!in_rth(h,m,s,S)) continue;

            float bid, ask; int32_t bs, asz;
            if(!parse_float(sbid,bid) || !parse_float(sask,ask) ||
               !parse_int32(sbs,bs) || !parse_int32(sas,asz)){
                G.bump("parse_fail",h); continue;
            }
            if(bid<=0 || ask<=0 || bs<=0 || asz<=0){ G.bump("nonpos_field",h); continue; }

            int msec=0;
            if(time.size()>=12){
                int32_t tms=0; parse_int32(time.substr(9,3), tms); msec=tms;
            }
            uint64_t d64=0; if(!parse_u64(date,d64)) continue;
            uint64_t ts = d64*1000000000ULL + (uint64_t)h*10000000ULL + (uint64_t)m*100000ULL + (uint64_t)s*1000ULL + (uint64_t)msec;

            if(bucket.ms==0) bucket.reset(ts);

            if(ts != bucket.ms){
                Row r; float new_mid=0.f;
                bool ok=bucket.out(r, have_prev?prev_mid:0.f, true, new_mid);
                if(ok){
                    if(!have_prev || nbbo::ymd(r.ts)!=prev_date) r.logret = std::numeric_limits<float>::quiet_NaN();

                    if(S.clock_grid && S.ffill && have_prev_row){
                        if(nbbo::same_day(last_emit,r.ts)){
                            int gap = nbbo::ms_since_midnight(r.ts) - nbbo::ms_since_midnight(last_emit) - 1;
                            if(gap>0 && gap<=S.max_ffill_gap_ms){
                                uint64_t t=last_emit;
                                for(int g=0; g<gap; ++g){
                                    t=nbbo::inc_ms(t);
                                    Row f=prev_row; f.ts=t; f.logret=0.0f;
                                    MsBinRow br{ f.ts,f.mid,f.logret,f.bidSize,f.askSize,f.spread,f.bid,f.ask };
                                    bin.write((char*)&br, sizeof(br));
                                    if((++out_local % S.log_every_out)==0){
                                        auto tot = p_out.fetch_add(S.log_every_out, std::memory_order_relaxed) + S.log_every_out;
                                        std::cerr << "[stageA] " << csv.filename().string() << " out=" << tot << "\n";
                                    }
                                    last_emit=t;
                                }
                            } else if(gap>S.max_ffill_gap_ms) {
                                have_prev=false;
                            }
                        } else have_prev=false;
                    }

                    MsBinRow br{ r.ts,r.mid,r.logret,r.bidSize,r.askSize,r.spread,r.bid,r.ask };
                    bin.write((char*)&br, sizeof(br));
                    if((++out_local % S.log_every_out)==0){
                        auto tot = p_out.fetch_add(S.log_every_out, std::memory_order_relaxed) + S.log_every_out;
                        std::cerr << "[stageA] " << csv.filename().string() << " out=" << tot << "\n";
                    }
                    prev_mid=new_mid; prev_date=nbbo::ymd(r.ts); have_prev=true;
                    last_emit=r.ts; prev_row=r; have_prev_row=true;
                }
                bucket.reset(ts);
            }
            Quote q{ts,bid,ask,bs,asz,exs[0]};
            bucket.upd(q,G,h);
        }
        if(bucket.ms){
            Row r; float new_mid=0.f;
            bool ok=bucket.out(r, have_prev?prev_mid:0.f, true, new_mid);
            if(ok){
                if(!have_prev || nbbo::ymd(r.ts)!=prev_date) r.logret = std::numeric_limits<float>::quiet_NaN();
                MsBinRow br{ r.ts,r.mid,r.logret,r.bidSize,r.askSize,r.spread,r.bid,r.ask };
                bin.write((char*)&br, sizeof(br));
            }
        }

        bin.close();
        std::lock_guard<std::mutex> lk(gl_mu);
        gl_total.merge(G);
    }

    // List CSVs (optional). Empty result is acceptable now.
    std::vector<fs::path> list_csv() const {
        std::vector<fs::path> v;
        std::error_code ec;
        if(!S.in_dir.empty() && fs::exists(S.in_dir, ec) && fs::is_directory(S.in_dir, ec)){
            for(auto& e: fs::directory_iterator(S.in_dir)){
                if(!e.is_regular_file()) continue;
                auto p=e.path(); auto nm=p.filename().string();
                if(nm.empty() || nm[0]=='.') continue;
                if(nm.rfind(".csv.gz")!=nm.size()-7) continue;
                if(!S.sym_root.empty() && !starts_with(nm, S.sym_root)) continue;
                int yr = extract_year(nm, S.sym_root);
                if(yr<0) continue;
                if(S.year_lo && yr<S.year_lo) continue;
                if(S.year_hi && yr>S.year_hi) continue;
                v.push_back(p);
            }
            std::sort(v.begin(), v.end(), [](const fs::path& a, const fs::path& b){
                return a.filename() < b.filename();
            });
        }
        return v;
    }

    // From CSV list: map to expected msbins in the appropriate cache subdir.
    bool msbins_from_csv_list(const std::vector<fs::path>& csv_files, std::vector<fs::path>& out){
        out.clear();
        if(csv_files.empty()) return false;
        auto sub = cache_subdir();
        if(!fs::exists(sub)) return false;
        for(const auto& csv : csv_files){
            auto msb = msbin_path_for_csv(csv);
            if(!fs::exists(msb)) return false;
            out.push_back(msb);
        }
        sort_chronologically(S, out);
        return !out.empty();
    }

    // Generic scan of a specific subdir for msbins matching sym_root/years.
    bool msbins_from_subdir(const fs::path& subdir, std::vector<fs::path>& out){
        out.clear();
        std::error_code ec;
        if(!fs::exists(subdir, ec) || !fs::is_directory(subdir, ec)) return false;
        for(auto& e: fs::directory_iterator(subdir)){
            if(!e.is_regular_file()) continue;
            auto p = e.path();
            if(p.extension() != ".msbin") continue;
            auto nm = p.filename().string();
            if(!starts_with(nm, S.sym_root)) continue;
            int yr = extract_year(nm, S.sym_root);
            if(yr<0) continue;
            if(S.year_lo && yr<S.year_lo) continue;
            if(S.year_hi && yr>S.year_hi) continue;
            out.push_back(p);
        }
        sort_chronologically(S, out);
        return !out.empty();
    }

    // Cache-only: scan cache subdir for msbins (depending on mode).
    bool msbins_from_cache_only(std::vector<fs::path>& out){
        return msbins_from_subdir(cache_subdir(), out);
    }

    // Build Stage A in parallel into the correct cache subdir (event or clock)
    void parallel_csv_to_msbin(const std::vector<fs::path>& files){
        std::atomic<size_t> idx{0};
        auto worker = [&](){
            while(true){
                size_t i = idx.fetch_add(1);
                if(i>=files.size()) break;
                const auto& csv = files[i];
                auto out = msbin_path_for_csv(csv);
                fs::create_directories(out.parent_path());
                std::cerr << "[stageA] " << (i+1) << "/" << files.size()
                          << " -> " << out.filename().string() << "\n";
                process_file_to_msbin(csv, out);
            }
        };
        std::vector<std::thread> pool;
        int W = std::max(1, S.workers);
        for(int i=0;i<W;++i) pool.emplace_back(worker);
        for(auto& t: pool) t.join();
    }

    /******** Event→Clock ffill fallback (from ms_event to ms_clock) ********/
    void event_to_clock_ffill_parallel(const std::vector<fs::path>& ms_event_bins,
                                       std::vector<fs::path>& ms_clock_bins_out) {
        ms_clock_bins_out.clear();
        fs::path outdir = S.cache_dir / "ms_clock";
        fs::create_directories(outdir);

        std::atomic<size_t> idx{0};

        auto convert_one = [&](const fs::path& in_path){
            std::ifstream in(in_path, std::ios::binary);
            if(!in) throw std::runtime_error("cannot open ms_event for read: " + in_path.string());
            fs::path out_path = outdir / in_path.filename();
            std::ofstream out(out_path, std::ios::binary);
            if(!out) throw std::runtime_error("cannot open ms_clock for write: " + out_path.string());

            MsBinRow prev{}; bool have_prev=false;
            uint64_t last_emit_ts=0;
            MsBinRow r{};
            uint64_t wrote=0, read=0;

            while(in.read((char*)&r, sizeof(r))){
                ++read;
                if(have_prev){
                    if(nbbo::same_day(last_emit_ts, r.ts)){
                        int gap = nbbo::ms_since_midnight(r.ts) - nbbo::ms_since_midnight(last_emit_ts) - 1;
                        if(gap>0 && gap<=S.max_ffill_gap_ms){
                            uint64_t t = last_emit_ts;
                            for(int g=0; g<gap; ++g){
                                t = nbbo::inc_ms(t);
                                MsBinRow f = prev;
                                f.ts = t;
                                f.logret = 0.0f;
                                out.write((char*)&f, sizeof(f));
                                if((++wrote % 10'000'000ULL)==0){
                                    std::cerr << "[ffill-from-event] " << in_path.filename().string()
                                              << " wrote=" << wrote << "\n";
                                }
                            }
                        }
                    }
                }
                out.write((char*)&r, sizeof(r));
                if((++wrote % 10'000'000ULL)==0){
                    std::cerr << "[ffill-from-event] " << in_path.filename().string()
                              << " wrote=" << wrote << "\n";
                }

                prev = r; have_prev = true; last_emit_ts = r.ts;
            }
            out.close();
            std::cerr << "[ffill-from-event] done " << in_path.filename().string()
                      << " (+read=" << read << ", wrote=" << wrote << ") -> " << out_path.filename().string() << "\n";
            return out_path;
        };

        int W = std::max(1, S.workers);
        std::mutex mu_append;
        std::vector<fs::path> produced(ms_event_bins.size());

        auto worker = [&](){
            while(true){
                size_t i = idx.fetch_add(1);
                if(i>=ms_event_bins.size()) break;
                const auto& in_path = ms_event_bins[i];
                auto out_path = convert_one(in_path);
                std::lock_guard<std::mutex> lk(mu_append);
                produced[i] = out_path;
            }
        };

        std::vector<std::thread> pool;
        for(int t=0;t<W;++t) pool.emplace_back(worker);
        for(auto& t: pool) t.join();

        for(const auto& p : produced) if(!p.empty()) ms_clock_bins_out.push_back(p);
        sort_chronologically(S, ms_clock_bins_out);
    }

    /******** Fast tail-quantile winsor (exact for extreme tails) ********/
    void tail_quantiles_parallel(const std::vector<fs::path>& msbins, double& cut_lo, double& cut_hi){
        using MaxHeap = std::priority_queue<float>; // keep smallest L values (max-heap)
        using MinHeap = std::priority_queue<float, std::vector<float>, std::greater<float>>; // keep largest L values
        const size_t L = 200'000; // adjust if you like; still tiny

        std::atomic<size_t> idx{0};
        std::atomic<unsigned long long> N_finite{0};

        MaxHeap global_lows;  MinHeap global_highs;
        std::mutex mu;

        auto push_low = [&](MaxHeap& hp, float v){
            if(hp.size()<L) hp.push(v);
            else if(v < hp.top()){ hp.pop(); hp.push(v); }
        };
        auto push_high = [&](MinHeap& hp, float v){
            if(hp.size()<L) hp.push(v);
            else if(v > hp.top()){ hp.pop(); hp.push(v); }
        };

        auto worker = [&](){
            MaxHeap loc_low; MinHeap loc_high;
            unsigned long long locN=0ULL;

            while(true){
                size_t i = idx.fetch_add(1);
                if(i>=msbins.size()) break;
                const auto& p = msbins[i];
                std::ifstream in(p, std::ios::binary);
                if(!in) throw std::runtime_error("cannot open msbin: "+p.string());
                MsBinRow r; uint64_t processed=0;
                while(in.read((char*)&r, sizeof(r))){
                    if(std::isfinite(r.logret)){
                        ++locN;
                        push_low(loc_low,  r.logret);
                        push_high(loc_high, r.logret);
                    }
                    if((++processed % 20'000'000ULL)==0){
                        std::cerr << "[pass-TAIL] " << p.filename().string() << " rows=" << processed << "\n";
                    }
                }
                {
                    std::lock_guard<std::mutex> lk(mu);
                    N_finite += locN;
                    while(!loc_low.empty()){ push_low(global_lows, loc_low.top()); loc_low.pop(); }
                    while(!loc_high.empty()){ push_high(global_highs, loc_high.top()); loc_high.pop(); }
                }
                std::cerr << "[pass-TAIL] done " << (i+1) << "/" << msbins.size()
                          << " " << p.filename().string() << " (+ finite=" << locN << ")\n";
            }
        };

        int W = std::max(1, S.workers);
        std::vector<std::thread> pool; pool.reserve(W);
        for(int t=0;t<W;++t) pool.emplace_back(worker);
        for(auto& t: pool) t.join();

        const unsigned long long N = N_finite.load();
        if(N==0){ cut_lo = cut_hi = std::numeric_limits<double>::quiet_NaN(); return; }

        std::vector<float> lows; lows.reserve(global_lows.size());
        while(!global_lows.empty()){ lows.push_back(global_lows.top()); global_lows.pop(); }
        std::sort(lows.begin(), lows.end());

        std::vector<float> highs; highs.reserve(global_highs.size());
        while(!global_highs.empty()){ highs.push_back(global_highs.top()); global_highs.pop(); }
        std::sort(highs.begin(), highs.end());

        const unsigned long long r_lo = (unsigned long long) std::floor(S.q_lo * (double)N);
        const unsigned long long r_hi = (unsigned long long) std::floor(S.q_hi * (double)N);

        size_t idx_lo = (r_lo < lows.size()) ? (size_t)r_lo : (lows.empty()?0:lows.size()-1);

        unsigned long long base = (highs.size() < (size_t)N) ? (N - (unsigned long long)highs.size()) : 0ULL;
        size_t idx_hi = (r_hi <= base) ? 0 : (size_t)std::min<unsigned long long>(r_hi - base, highs.size()-1);

        cut_lo = (double) (lows.empty()? std::numeric_limits<float>::quiet_NaN() : lows[idx_lo]);
        cut_hi = (double) (highs.empty()? std::numeric_limits<float>::quiet_NaN() : highs[idx_hi]);

        std::cerr << "[pass-TAIL] N=" << N
                  << " q_lo=" << S.q_lo << " -> rank " << r_lo << " cutoff " << cut_lo
                  << " | q_hi=" << S.q_hi << " -> rank " << r_hi << " cutoff " << cut_hi << "\n";
    }

    /******** Parquet writer: partitioned by year into out/<mode>/SYM_YYYY.parquet ********/
    struct YearWriter {
        int year;
        std::shared_ptr<arrow::io::OutputStream> out;
        std::unique_ptr<parquet::arrow::FileWriter> writer;

        arrow::UInt64Builder tsb;
        arrow::FloatBuilder  midb, lrb, bsb, asb, sprb, bidb, askb;

        int64_t  nrows_batch = 0;
        uint64_t total_rows  = 0;

        YearWriter(const YearWriter&)            = delete;
        YearWriter& operator=(const YearWriter&) = delete;
        YearWriter(YearWriter&&)                 = default;
        YearWriter& operator=(YearWriter&&)      = default;

        explicit YearWriter(
            int yr,
            std::shared_ptr<arrow::io::OutputStream> o,
            std::unique_ptr<parquet::arrow::FileWriter> w
        )
        : year(yr),
          out(std::move(o)),
          writer(std::move(w)),
          tsb(arrow::default_memory_pool()),
          midb(arrow::default_memory_pool()),
          lrb(arrow::default_memory_pool()),
          bsb(arrow::default_memory_pool()),
          asb(arrow::default_memory_pool()),
          sprb(arrow::default_memory_pool()),
          bidb(arrow::default_memory_pool()),
          askb(arrow::default_memory_pool())
        {}

        void flush_batch(const std::shared_ptr<arrow::Schema>& schema){
            if(nrows_batch==0) return;
            auto batch = arrow::RecordBatch::Make(schema, nrows_batch, {
                tsb.Finish().ValueOrDie(), midb.Finish().ValueOrDie(),
                lrb.Finish().ValueOrDie(), bsb.Finish().ValueOrDie(),
                asb.Finish().ValueOrDie(), sprb.Finish().ValueOrDie(),
                bidb.Finish().ValueOrDie(), askb.Finish().ValueOrDie()
            });
            ARROW_OK(writer->WriteRecordBatch(*batch));
            total_rows += (uint64_t)nrows_batch;
            nrows_batch = 0;

            // Reuse builders
            tsb.Reset(); midb.Reset(); lrb.Reset(); bsb.Reset(); asb.Reset(); sprb.Reset(); bidb.Reset(); askb.Reset();

            if((total_rows % 2'000'000ULL)==0){
                std::cerr << "[pass-Parquet] year=" << year << " wrote rows=" << total_rows << "\n";
            }
        }
        void close(const std::shared_ptr<arrow::Schema>& schema){
            flush_batch(schema);
            ARROW_OK(writer->Close());
            ARROW_OK(out->Close());
            std::cerr << "[pass-Parquet] year=" << year << " total=" << total_rows << " (closed)\n";
        }
    };

    fs::path out_root_dir() const {
        if(S.out_parquet.empty()) throw std::runtime_error("--out required");
        fs::path p = S.out_parquet;
        if(p.has_extension() && p.extension()==".parquet") return p.parent_path();
        return p;
    }
    std::string out_mode_dirname() const {
        if(S.clock_grid) return S.winsorize? "clock_winsor" : "clock";
        return S.winsorize? "event_winsor" : "event";
    }

    void msbins_to_parquet_per_year(const std::vector<fs::path>& msbins, double cut_lo, double cut_hi){
        constexpr int64_t BATCH=2'000'000;

        auto schema = nbbo::nbbo_schema();

        fs::path base = out_root_dir() / out_mode_dirname();
        fs::create_directories(base);

        auto open_year = [&](int yr) -> std::unique_ptr<YearWriter> {
            fs::path path = base / (S.sym_root + "_" + std::to_string(yr) + ".parquet");
            auto out_stream = arrow::io::FileOutputStream::Open(path.string()).ValueOrDie();
            auto fw = parquet::arrow::FileWriter::Open(*schema, arrow::default_memory_pool(), out_stream).ValueOrDie();
            std::cerr << "[pass-Parquet] open year=" << yr << " -> " << path.filename().string() << "\n";
            return std::make_unique<YearWriter>(yr, std::move(out_stream), std::move(fw));
        };

        std::map<int, std::unique_ptr<YearWriter>> writers;

        auto get_writer = [&](int yr) -> YearWriter& {
            auto it = writers.find(yr);
            if(it == writers.end()){
                auto ptr = open_year(yr);
                auto [pos, _] = writers.emplace(yr, std::move(ptr));
                return *pos->second;
            }
            return *it->second;
        };

        uint64_t global_rows=0;

        for(size_t i=0;i<msbins.size();++i){
            const auto& p = msbins[i];
            std::ifstream in(p, std::ios::binary);
            if(!in) throw std::runtime_error("cannot open msbin: " + p.string());
            MsBinRow r; uint64_t loc=0;

            std::cerr << "[pass-Parquet] " << (i+1) << "/" << msbins.size()
                      << " " << p.filename().string() << " -> partitioned years\n";

            while(in.read((char*)&r, sizeof(r))){
                // Winsor policy
                if(S.winsorize && std::isfinite(r.logret)){
                    if(S.winsor_clip){
                        float lr = r.logret;
                        if(lr < cut_lo) r.logret = (float)cut_lo;
                        else if(lr > cut_hi) r.logret = (float)cut_hi;
                    } else {
                        if(r.logret < cut_lo || r.logret > cut_hi) { ++loc; continue; } // drop
                    }
                }

                int yr = nbbo::year_from_ts(r.ts);
                YearWriter& yw = get_writer(yr);

                ARROW_OK(yw.tsb.Append(r.ts));
                ARROW_OK(yw.midb.Append(r.mid));
                if(std::isfinite(r.logret)) ARROW_OK(yw.lrb.Append(r.logret)); else ARROW_OK(yw.lrb.AppendNull());
                ARROW_OK(yw.bsb.Append(r.bidSize));
                ARROW_OK(yw.asb.Append(r.askSize));
                ARROW_OK(yw.sprb.Append(r.spread));
                ARROW_OK(yw.bidb.Append(r.bid));
                ARROW_OK(yw.askb.Append(r.ask));

                if(++yw.nrows_batch>=BATCH){
                    yw.flush_batch(schema);
                }

                if(((++global_rows) % 5'000'000ULL)==0){
                    std::cerr << "[pass-Parquet] total_written=" << global_rows << "\n";
                }
                ++loc;
            }
        }

        for(auto& [yr, ptr] : writers){
            ptr->close(schema);
        }
        std::cerr << "[pass-Parquet] partitioned write complete. files=" << writers.size()
                  << " out_dir=" << (out_root_dir()/out_mode_dirname()) << "\n";
    }

    void run(){
        if(S.cache_dir.empty()) throw std::runtime_error("--cache DIR required");
        fs::create_directories(S.cache_dir / "ms_event");
        fs::create_directories(S.cache_dir / "ms_clock");

        std::cerr << "[cfg] grid=" << (S.event_grid? "event" : (S.clock_grid? "clock" : "unknown"))
                  << " ffill=" << (S.ffill? "on":"off")
                  << " winsor=" << (S.winsorize? (S.winsor_clip? "clip":"drop") : "off")
                  << " q=(" << S.q_lo << "," << S.q_hi << ") venues=";
        for(char c: S.venues) std::cerr<<c;
        std::cerr << " rth=" << std::setfill('0') << std::setw(2) << S.rth_start_h << ":" << std::setw(2) << S.rth_start_m
                  << "-"     << std::setw(2) << S.rth_end_h   << ":" << std::setw(2) << S.rth_end_m
                  << " max_ffill_gap_ms=" << S.max_ffill_gap_ms
                  << " workers=" << S.workers
                  << " sym_root=" << S.sym_root
                  << " years=" << (S.year_lo? std::to_string(S.year_lo):"-") << ":" << (S.year_hi? std::to_string(S.year_hi):"-")
                  << "\n";

        auto csv_files = list_csv();  // may be empty

        // Decide msbins
        std::vector<fs::path> msbins;
        bool have_cache = msbins_from_csv_list(csv_files, msbins);
        if(!have_cache) have_cache = msbins_from_cache_only(msbins);

        // Fallback: synthesize ms_clock from ms_event if needed
        if(!have_cache && S.clock_grid){
            std::vector<fs::path> ms_event_bins;
            bool have_event_cache = msbins_from_subdir(S.cache_dir / "ms_event", ms_event_bins);
            if(have_event_cache){
                std::cerr << "▶ [ffill-from-event] ms_clock cache missing; synthesizing from ms_event ("
                          << ms_event_bins.size() << " files) with gap<=" << S.max_ffill_gap_ms << "ms...\n";
                std::vector<fs::path> produced_clock;
                event_to_clock_ffill_parallel(ms_event_bins, produced_clock);
                if(!produced_clock.empty()){
                    msbins = produced_clock;
                    have_cache = true;
                    std::cerr << "▶ [ffill-from-event] done. Created " << produced_clock.size()
                              << " files in " << (S.cache_dir / "ms_clock") << "\n";
                }
            }
        }

        auto t0 = std::chrono::steady_clock::now();
        if(have_cache){
            std::cerr << "▶ [stageA] skipped: found msbin cache (" << msbins.size()
                      << " files) in " << cache_subdir() << "\n";
        } else {
            if(csv_files.empty()){
                throw std::runtime_error("No CSVs found in --in and no msbins in " + cache_subdir().string());
            }
            std::cerr << "▶ [stageA] build: generating msbins into " << cache_subdir() << "\n";
            parallel_csv_to_msbin(csv_files);
            if(!msbins_from_csv_list(csv_files, msbins) && !msbins_from_cache_only(msbins)){
                throw std::runtime_error("Stage A built nothing usable in " + cache_subdir().string());
            }
        }
        auto t1 = std::chrono::steady_clock::now();
        std::cerr << "[stageA] elapsed=" << std::chrono::duration<double>(t1-t0).count() << "s\n";

        // Stage B: fast tail-quantiles
        double cut_lo=-INFINITY, cut_hi=INFINITY;
        if(S.winsorize){
            std::cerr << "▶ [pass-TAIL] computing extreme quantiles in parallel (" << S.workers << " threads)...\n";
            tail_quantiles_parallel(msbins, cut_lo, cut_hi);
        }

        // Stage C/D: partitioned write
        auto t2 = std::chrono::steady_clock::now();
        std::cerr << "▶ [pass-Parquet] writing per-year into " << (out_root_dir()/out_mode_dirname()) << "...\n";
        msbins_to_parquet_per_year(msbins, cut_lo, cut_hi);
        auto t3 = std::chrono::steady_clock::now();

        std::cerr << "[stageB+C+D] elapsed=" << std::chrono::duration<double>(t3-t2).count() << "s\n";

        if(!S.report_path.empty()) gl_total.write_report(S.report_path);
        std::cerr<<"✅ Completed. Output dir: "<<(out_root_dir()/out_mode_dirname())<<"\n";
        if(!S.report_path.empty()) std::cerr<<"Report: "<<S.report_path<<"\n";
    }
};

/************** CLI ********************/
int main(int argc,char** argv){
    Settings S;
    if(argc<5){ usage(); return 1; }
    for(int i=1;i<argc;++i){
        string a=argv[i]; auto need=[&](int n){ if(i+n>=argc){ usage(); std::exit(2);} };
        if(a=="--in"){ need(1); S.in_dir=argv[++i]; }
        else if(a=="--cache"){ need(1); S.cache_dir=argv[++i]; }
        else if(a=="--out"){ need(1); S.out_parquet=argv[++i]; }
        else if(a=="--report"){ need(1); S.report_path=argv[++i]; }
        else if(a=="--clock"){ S.clock_grid=true; S.event_grid=false; }
        else if(a=="--event"){ S.event_grid=true; S.clock_grid=false; S.ffill=false; }
        else if(a=="--ffill"){ S.ffill=true; S.clock_grid=true; }
        else if(a=="--no-ffill"){ S.ffill=false; }
        else if(a=="--max-ffill-gap-ms"){ need(1); S.max_ffill_gap_ms=std::stoi(argv[++i]); }
        else if(a=="--winsor"){ S.winsorize=true; }
        else if(a=="--winsor-clip"){ S.winsor_clip=true; S.winsorize=true; }
        else if(a=="--winsor-drop"){ S.winsor_clip=false; S.winsorize=true; }
        else if(a=="--winsor-quantiles"){ need(1); string q=argv[++i]; auto c=q.find(','); S.q_lo=std::stod(q.substr(0,c)); S.q_hi=std::stod(q.substr(c+1)); }
        else if(a=="--rth"){ need(1); string w=argv[++i]; auto d=w.find('-'); string s=w.substr(0,d), e=w.substr(d+1); int hs,ms,ss, he,me,se; parse_time_hms(s,hs,ms,ss); parse_time_hms(e,he,me,se); S.rth_start_h=hs; S.rth_start_m=ms; S.rth_end_h=he; S.rth_end_m=me; }
        else if(a=="--ex"){ need(1); S.venues.clear(); for(char c: string(argv[++i])) S.venues.insert(c); }
        else if(a=="--stale-ms"){ need(1); S.stale_ms=std::stoi(argv[++i]); }
        else if(a=="--log-every-in"){ need(1); S.log_every_in=std::stoull(argv[++i]); }
        else if(a=="--log-every-out"){ need(1); S.log_every_out=std::stoull(argv[++i]); }
        else if(a=="--sym-root"){ need(1); S.sym_root=argv[++i]; }
        else if(a=="--years"){ need(1); string y=argv[++i]; auto c=y.find(':'); S.year_lo=std::stoi(y.substr(0,c)); S.year_hi=std::stoi(y.substr(c+1)); }
        else if(a=="--workers"){ need(1); S.workers=std::stoi(argv[++i]); }
        else { std::cerr<<"Unknown arg: "<<a<<"\n"; usage(); return 1; }
    }
    try{
        Pipeline P{S}; P.run();
    } catch(const std::exception& e){
        std::cerr<<"FATAL: "<<e.what()<<"\n"; return 2;
    }
    return 0;
}
