// nbbo_pipeline/src/nbbo.backtester.cppm
//
// C++20 module interface that wraps the existing nbbo/backtester.hpp
// and exports its declarations as module nbbo.backtester.
//
// This lets users do:
//   import nbbo.backtester;
// instead of #include "nbbo/backtester.hpp".

export module nbbo.backtester;

// Export everything declared in the legacy header.
export {
  #include "nbbo/backtester.hpp"
}
