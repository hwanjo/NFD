#include "decent-strategy.hpp"
#include "algorithm.hpp"
#include "common/logger.hpp"

#include <inttypes.h>
#include <ndn-cxx/util/time.hpp>
#include <ndn-cxx/decent-name.hpp>
#include <ndn-cxx/encoding/block-helpers.hpp>
#include <iostream>
#include <ctime>
#include <ratio>
#include <chrono>

namespace nfd {
namespace fw {

NFD_LOG_INIT(DecentStrategy);
NFD_REGISTER_STRATEGY(DecentStrategy);

const time::milliseconds DecentStrategy::RETX_SUPPRESSION_INITIAL(10);
const time::milliseconds DecentStrategy::RETX_SUPPRESSION_MAX(250);

DecentStrategy::DecentStrategy(Forwarder& forwarder, const Name& name)
  : Strategy(forwarder)
  , ProcessNackTraits(this)
  , m_retxSuppression(RETX_SUPPRESSION_INITIAL,
                      RetxSuppressionExponential::DEFAULT_MULTIPLIER,
                      RETX_SUPPRESSION_MAX)
{
  ParsedInstanceName parsed = parseInstanceName(name);
  if (!parsed.parameters.empty()) {
    NDN_THROW(std::invalid_argument("DecentStrategy does not accept parameters"));
  }
  if (parsed.version && *parsed.version != getStrategyName()[-1].toVersion()) {
    NDN_THROW(std::invalid_argument(
      "DecentStrategy does not support version " + to_string(*parsed.version)));
  }
  this->setInstanceName(makeInstanceName(name, getStrategyName()));
}

const Name&
DecentStrategy::getStrategyName()
{
  static Name strategyName("/localhost/nfd/strategy/decent/%FD%05");
  return strategyName;
}

void
DecentStrategy::afterReceiveInterest(const FaceEndpoint& ingress, const Interest& interest,
                                         const shared_ptr<pit::Entry>& pitEntry)
{
  RetxSuppressionResult suppression = m_retxSuppression.decidePerPitEntry(*pitEntry);
  if (suppression == RetxSuppressionResult::SUPPRESS) {
    NFD_LOG_DEBUG(interest << " from=" << ingress << " suppressed");
    return;
  }

  const fib::Entry& fibEntry = this->lookupFib(*pitEntry);
  const fib::NextHopList& nexthops = fibEntry.getNextHops();
  auto it = nexthops.end();

  if (suppression == RetxSuppressionResult::NEW) {
    // forward to nexthop with lowest cost except downstream
    it = std::find_if(nexthops.begin(), nexthops.end(), [&] (const auto& nexthop) {
      return isNextHopEligible(ingress.face, interest, nexthop, pitEntry);
    });

    if (it == nexthops.end()) {
      NFD_LOG_DEBUG(interest << " from=" << ingress << " noNextHop");

      lp::NackHeader nackHeader;
      nackHeader.setReason(lp::NackReason::NO_ROUTE);
      this->sendNack(pitEntry, ingress, nackHeader);

      this->rejectPendingInterest(pitEntry);
      return;
    }

    auto egress = FaceEndpoint(it->getFace(), 0);
    NFD_LOG_DEBUG(interest << " from=" << ingress << " newPitEntry-to=" << egress);

		Interest decentInterest = interest;
		DecentName decentName(pitEntry->getDecentName().toUri());
		decentInterest.setDecentName(decentName);

		//std::cout << "[" << time::toIsoString(time::system_clock::now()) << "] DecentStrategy Send" << std::endl;
		//std::cout << "[" << ndn::time::GetTimeMillis() << "] PIT Interest : " << pitEntry->getName().toUri().c_str() << std::endl;

    this->sendInterest(pitEntry, egress, decentInterest);
    return;
  }

  // find an unused upstream with lowest cost except downstream
  it = std::find_if(nexthops.begin(), nexthops.end(), [&] (const auto& nexthop) {
    return isNextHopEligible(ingress.face, interest, nexthop, pitEntry, true, time::steady_clock::now());
  });

  if (it != nexthops.end()) {
    auto egress = FaceEndpoint(it->getFace(), 0);
    this->sendInterest(pitEntry, egress, interest);
    NFD_LOG_DEBUG(interest << " from=" << ingress << " retransmit-unused-to=" << egress);
    return;
  }

  // find an eligible upstream that is used earliest
  it = findEligibleNextHopWithEarliestOutRecord(ingress.face, interest, nexthops, pitEntry);
  if (it == nexthops.end()) {
    NFD_LOG_DEBUG(interest << " from=" << ingress << " retransmitNoNextHop");
  }
  else {
    auto egress = FaceEndpoint(it->getFace(), 0);
    this->sendInterest(pitEntry, egress, interest);
    NFD_LOG_DEBUG(interest << " from=" << ingress << " retransmit-retry-to=" << egress);
  }
}

void
DecentStrategy::afterReceiveNack(const FaceEndpoint& ingress, const lp::Nack& nack,
                                     const shared_ptr<pit::Entry>& pitEntry)
{
  this->processNack(ingress.face, nack, pitEntry);
}

} // namespace fw
} // namespace nfd
