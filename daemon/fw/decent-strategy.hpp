#ifndef NFD_DAEMON_FW_DECENT_STRATEGY_HPP
#define NFD_DAEMON_FW_DECENT_STRATEGY_HPP

#include "strategy.hpp"
#include "process-nack-traits.hpp"
#include "retx-suppression-exponential.hpp"

namespace nfd {
namespace fw {

class DecentStrategy : public Strategy
                         , public ProcessNackTraits<DecentStrategy>
{
public:
  explicit
  DecentStrategy(Forwarder& forwarder, const Name& name = getStrategyName());

  static const Name&
  getStrategyName();

  void
  afterReceiveInterest(const FaceEndpoint& ingress, const Interest& interest,
                       const shared_ptr<pit::Entry>& pitEntry) override;

  void
  afterReceiveNack(const FaceEndpoint& ingress, const lp::Nack& nack,
                   const shared_ptr<pit::Entry>& pitEntry) override;

PUBLIC_WITH_TESTS_ELSE_PRIVATE:
  static const time::milliseconds RETX_SUPPRESSION_INITIAL;
  static const time::milliseconds RETX_SUPPRESSION_MAX;
  RetxSuppressionExponential m_retxSuppression;

  friend ProcessNackTraits<DecentStrategy>;
};

} // namespace fw
} // namespace nfd

#endif // NFD_DAEMON_FW_DECENT_STRATEGY_HPP
