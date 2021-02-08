/*
 * Copyright (c) 2014-2019,  Regents of the University of California,
 *                           Arizona Board of Regents,
 *                           Colorado State University,
 *                           University Pierre & Marie Curie, Sorbonne University,
 *                           Washington University in St. Louis,
 *                           Beijing Institute of Technology,
 *                           The University of Memphis.
 *
 * This file is part of NFD (Named Data Networking Forwarding Daemon).
 * See AUTHORS.md for complete list of NFD authors and contributors.
 *
 * NFD is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 *
 * NFD is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * NFD, e.g., in COPYING.md file.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "name-tree.hpp"
#include "common/logger.hpp"

#include <boost/concept/assert.hpp>
#include <boost/concept_check.hpp>
#include <type_traits>

namespace nfd {
namespace name_tree {

NFD_LOG_INIT(NameTree);

NameTree::NameTree(size_t nBuckets)
  : m_ht(HashtableOptions(nBuckets))
{
}

Entry&
NameTree::lookup(const Name& name, size_t prefixLen)
{
  NFD_LOG_TRACE("lookup(" << name << ", " << prefixLen << ')');
  BOOST_ASSERT(prefixLen <= name.size());
  BOOST_ASSERT(prefixLen <= getMaxDepth());

  HashSequence hashes = computeHashes(name, prefixLen);
  const Node* node = nullptr;
  Entry* parent = nullptr;

  for (size_t i = 0; i <= prefixLen; ++i) {
    bool isNew = false;
    std::tie(node, isNew) = m_ht.insert(name, i, hashes);

    if (isNew && parent != nullptr) {
      node->entry.setParent(*parent);
    }
    parent = &node->entry;
  }
  return node->entry;
}

Entry&
NameTree::lookup(const fib::Entry& fibEntry)
{
  NFD_LOG_TRACE("lookup(FIB " << fibEntry.getPrefix() << ')');
  Entry* nte = this->getEntry(fibEntry);
  if (nte == nullptr) {
    // special case: Fib::s_emptyEntry is unattached
    BOOST_ASSERT(fibEntry.getPrefix().empty());
    return this->lookup(fibEntry.getPrefix());
  }

  BOOST_ASSERT(nte->getFibEntry() == &fibEntry);
  return *nte;
}

Entry&
NameTree::lookup(const pit::Entry& pitEntry)
{
  const Name& name = pitEntry.getName();
  NFD_LOG_TRACE("lookup(PIT " << name << ')');
  bool hasDigest = name.size() > 0 && name[-1].isImplicitSha256Digest();
  if (hasDigest && name.size() <= getMaxDepth()) {
    return this->lookup(name);
  }

  Entry* nte = this->getEntry(pitEntry);
  BOOST_ASSERT(nte != nullptr);
  BOOST_ASSERT(std::count_if(nte->getPitEntries().begin(), nte->getPitEntries().end(),
                             [&pitEntry] (const auto& pitEntry1) {
                               return pitEntry1.get() == &pitEntry;
                             }) == 1);
  return *nte;
}

Entry&
NameTree::lookup(const measurements::Entry& measurementsEntry)
{
  NFD_LOG_TRACE("lookup(M " << measurementsEntry.getName() << ')');
  Entry* nte = this->getEntry(measurementsEntry);
  BOOST_ASSERT(nte != nullptr);

  BOOST_ASSERT(nte->getMeasurementsEntry() == &measurementsEntry);
  return *nte;
}

Entry&
NameTree::lookup(const strategy_choice::Entry& strategyChoiceEntry)
{
  NFD_LOG_TRACE("lookup(SC " << strategyChoiceEntry.getPrefix() << ')');
  Entry* nte = this->getEntry(strategyChoiceEntry);
  BOOST_ASSERT(nte != nullptr);

  BOOST_ASSERT(nte->getStrategyChoiceEntry() == &strategyChoiceEntry);
  return *nte;
}

Entry*
NameTree::lookupAndInsertDecent(const Name& name, size_t prefixLen)
{
  NFD_LOG_TRACE("lookup(" << name << ", " << prefixLen << ')');
  BOOST_ASSERT(prefixLen <= name.size());
  BOOST_ASSERT(prefixLen <= getMaxDepth());

	if (name.isNonPreemptibleName(name) || name.isUntaggedName(name))
	{
		// nonpreemptible name of untagged name
		return lookupAndInsertFastPath(name, prefixLen);
	}
	else {
		// has nonpreemptible name
		return lookupAndInsertSlowPath(name, prefixLen);
	}

	return nullptr;
}

Entry*
NameTree::lookupAndInsertDecentPit(const Name& name, size_t prefixLen)
{
  NFD_LOG_TRACE("lookup(" << name << ", " << prefixLen << ')');
  BOOST_ASSERT(prefixLen <= name.size());
  BOOST_ASSERT(prefixLen <= getMaxDepth());

	if (name.isUntaggedName(name)) {
		return lookupAndInsertFastPathPit(name, prefixLen);
	} else if (name.isNonPreemptibleName(name)) {
		return lookupAndInsertFastPathPit(name, prefixLen);
	} else {
		// has nonpreemptible name
		return lookupAndInsertSlowPath(name, prefixLen);
	}
}

Entry*
NameTree::lookupAndInsertDecentPit(const DecentName& name, size_t prefixLen)
{
  NFD_LOG_TRACE("lookup(" << name << ", " << prefixLen << ')');
  BOOST_ASSERT(prefixLen <= name.size());
  BOOST_ASSERT(prefixLen <= getMaxDepth());

	return lookupAndInsertFastPathPit(name, prefixLen);
}

Entry*
NameTree::lookupAndInsertFastPath(const Name& name, size_t prefixLen)
{
	const Node* node = nullptr;
	HashValue hash = computeHash(name, prefixLen);

	node = m_ht.lookupHashTable(name, prefixLen, hash);

	if (node != nullptr) {
		if (name.isNonPreemptibleName(name)) {
			return &node->entry;
		} else {
			return node->entry.getDecentHead();
		}
	}

	Name pName = name.getSubName(0, prefixLen - 1);
	Entry *parentTreeEntry = lookupAndInsertFastPath(pName, pName.size());

	Entry *treeEntry = &lookup(name, prefixLen);
	const Name utagName = name.decentUntag(name);
	treeEntry->setNdnTreeEntry(&lookup(utagName));

	if (parentTreeEntry->getName().toUri().compare("/decent") == 0)
		setRelationshipFirst(parentTreeEntry, treeEntry, name);
	else
		setRelationshipSecond(parentTreeEntry, treeEntry, name);

	return treeEntry;
}

inline Entry*
NameTree::lookupAndInsertFastPathPit(const Name& name, size_t prefixLen)
{
	const Node* node = nullptr;
	HashValue hash = computeHash(name, prefixLen);

	node = m_ht.lookupHashTable(name, prefixLen, hash);

	if (node != nullptr) {
		if (name.isNonPreemptibleName(name)) {
			return &node->entry;
		} else {
			return node->entry.getDecentHead();
		}
	}

	Name pName = name.getSubName(0, prefixLen - 1);
	Entry *parentTreeEntry = lookupAndInsertFastPathPit(pName, pName.size());

	Entry *treeEntry = nullptr;
	if (name.isUntaggedName(name)) {
		Name cName = parentTreeEntry->getName();
		std::string uri = name.getSubName(prefixLen-1, 1).toUri() + "#ffffffffffff";
		cName.append(uri);

		node = m_ht.insert(cName, prefixLen);
		treeEntry = &node->entry;
		treeEntry->setParent(*parentTreeEntry);
	} else {
    node = m_ht.insert(name, prefixLen);
    treeEntry = &node->entry;
    treeEntry->setParent(*parentTreeEntry);
	}

	return treeEntry;
}

inline Entry*
NameTree::lookupAndInsertFastPathPit(const DecentName& name, size_t prefixLen)
{
	const Node* node = nullptr;
	HashValue hash = computeHash(name, prefixLen);

	node = m_ht.lookupHashTable(name, prefixLen, hash);

	if (node != nullptr)
		return &node->entry;

	DecentName pName = name.getSubName(0, prefixLen - 1);
	Entry *parentTreeEntry = lookupAndInsertFastPathPit(pName, pName.size());

	Entry *treeEntry = nullptr;
	node = m_ht.insert(name, prefixLen);
	treeEntry = &node->entry;
	treeEntry->setParent(*parentTreeEntry);

	return treeEntry;
}

Entry*
NameTree::lookupAndInsertSlowPath(const Name& name, size_t prefixLen)
{
	size_t count = name.decentNameParseCount(name);
	const Name prefix = name.getSubName(0, count);
	if (!prefix.isNonPreemptibleName(prefix))
		return nullptr;

	Name postfix = prefix;

	Entry *treeEntry = lookupAndInsertFastPathPit(prefix, prefix.size());

	for (size_t i = count; i < prefixLen; i++) {
		std::string uri = name.getSubName(i, 1).toUri();
		std::list<Entry*> decentList = treeEntry->getDecentMap(uri);
		Entry *head = nullptr;
		if (decentList.size() != 0) {
			head = decentList.front();
			std::string auth = head->getName().getSubName(i, 1).toUri();
			postfix.append(auth);
		} else {
			std::string auth = uri + "#ffffffffffff";
			Name newName = name.getSubName(0, i);
			postfix.append(auth);
			head = &lookup(postfix);
		}

		treeEntry = head;
	}

	return treeEntry;
}

bool decentSort(Entry *first, Entry *second)
{
	return (first->getName().toUri() < second->getName().toUri());
}

void
NameTree::setRelationshipFirst(Entry* parent, Entry* child, const Name& name)
{
	Name utagName = name.decentUntag(name);
	std::string uri = utagName.getSubName(utagName.size()-1, 1).toUri();

	std::list<Entry*> decentList = parent->getDecentMap(uri);
	if (decentList.size() != 0) {
		Entry *first = decentList.front();
		decentList.push_back(child);
		decentList.sort(decentSort);
		Entry *after = decentList.front();
		if (first->getName().toUri().compare(after->getName().toUri()) != 0) {
			child->setIsDecentHead(true);
			first->setIsDecentHead(false);
		}
		parent->updateDecentMap(uri, decentList);
	} else {
		std::list<Entry*> newDecentList;
		newDecentList.push_back(child);
		parent->setDecentMap(uri, newDecentList);
		child->setIsDecentHead(true);
	}

	if (child->getIsDecentHead()) {
		parent->setDecentHead(child);
		child->getNdnTreeEntry()->setDecentHead(child);
	}
}

void
NameTree::setRelationshipSecond(Entry* parent, Entry* child, const Name& name)
{
	Name utagName = name.decentUntag(name);
	std::string uri = utagName.getSubName(utagName.size()-1, 1).toUri();

	std::list<Entry*> decentList = parent->getDecentMap(uri);
	if (decentList.size() != 0) {
		Entry *first = decentList.front();
		decentList.push_back(child);
		decentList.sort(decentSort);
		Entry *after = decentList.front();
		if (first->getName().toUri().compare(after->getName().toUri()) != 0) {
			if (parent->getIsDecentHead()) {
				child->setIsDecentHead(true);
				first->setIsDecentHead(false);
			}
		}
		parent->updateDecentMap(uri, decentList);
	} else {
		std::list<Entry*> newDecentList;
		newDecentList.push_back(child);
		parent->setDecentMap(uri, newDecentList);
		if (parent->getIsDecentHead()) {
			child->setIsDecentHead(true);
		}
	}

	if (child->getIsDecentHead()) {
		if (parent->getIsDecentHead()) {
			parent->setDecentHead(child);
			child->getNdnTreeEntry()->setDecentHead(child);
		} else {
			child->setIsDecentHead(false);
		}
	}
}

size_t
NameTree::eraseIfEmpty(Entry* entry, bool canEraseAncestors)
{
  BOOST_ASSERT(entry != nullptr);

  size_t nErased = 0;
  for (Entry* parent = nullptr; entry != nullptr && entry->isEmpty(); entry = parent) {
    parent = entry->getParent();

    if (parent != nullptr) {
      entry->unsetParent();
    }

    m_ht.erase(getNode(*entry));
    ++nErased;

    if (!canEraseAncestors) {
      break;
    }
  }

  if (nErased == 0) {
    NFD_LOG_TRACE("not-erase " << entry->getName());
  }
  return nErased;
}

Entry*
NameTree::findExactMatch(const Name& name, size_t prefixLen) const
{
  prefixLen = std::min(name.size(), prefixLen);
  if (prefixLen > getMaxDepth()) {
    return nullptr;
  }

  const Node* node = m_ht.find(name, prefixLen);
  return node == nullptr ? nullptr : &node->entry;
}

Entry*
NameTree::findExactMatch(const DecentName& name, size_t prefixLen) const
{
  prefixLen = std::min(name.size(), prefixLen);
  if (prefixLen > getMaxDepth()) {
    return nullptr;
  }

  const Node* node = m_ht.find(name, prefixLen);
  return node == nullptr ? nullptr : &node->entry;
}

Entry*
NameTree::findLongestPrefixMatch(const Name& name, const EntrySelector& entrySelector) const
{
  size_t depth = std::min(name.size(), getMaxDepth());
  HashSequence hashes = computeHashes(name, depth);

  for (ssize_t i = depth; i >= 0; --i) {
    const Node* node = m_ht.find(name, i, hashes);
    if (node != nullptr && entrySelector(node->entry)) {
      return &node->entry;
    }
  }

  return nullptr;
}

Entry*
NameTree::findLongestPrefixMatch(const DecentName& name, const EntrySelector& entrySelector) const
{
  size_t depth = std::min(name.size(), getMaxDepth());
  HashSequence hashes = computeHashes(name, depth);

  for (ssize_t i = depth; i >= 0; --i) {
    const Node* node = m_ht.find(name, i, hashes);
    if (node != nullptr && entrySelector(node->entry)) {
      return &node->entry;
    }
  }

  return nullptr;
}

Entry*
NameTree::findLongestPrefixMatchDecent(const Name& name, const EntrySelector& entrySelector) const
{
	const Name utagName = name.decentUntag(name);
  size_t depth = std::min(utagName.size(), getMaxDepth());
  HashSequence hashes = computeHashes(utagName, depth);

  for (ssize_t i = depth; i >= 0; --i) {
    const Node* node = m_ht.find(utagName, i, hashes);
		if (node != nullptr && node->entry.getDecentHead()) {
			if (entrySelector(*node->entry.getDecentHead())) {
				return node->entry.getDecentHead();
			}
		} else if (node != nullptr && entrySelector(node->entry)) {
      return &node->entry;
    }
  }

  return nullptr;
}

Entry*
NameTree::findLongestPrefixMatch(const Entry& entry1, const EntrySelector& entrySelector) const
{
  Entry* entry = const_cast<Entry*>(&entry1);
  while (entry != nullptr) {
    if (entrySelector(*entry)) {
      return entry;
    }
    entry = entry->getParent();
  }
  return nullptr;
}

Entry*
NameTree::findLongestPrefixMatchDecent(const Entry& entry1, const EntrySelector& entrySelector) const
{
	Entry* entry = const_cast<Entry*>(&entry1);
	Entry* decent = nullptr;

  while (entry != nullptr) {
		if (entry->getName().isNonPreemptibleName(entry->getName())) {
			decent = entry;
			break;
		}
		decent = entry->getDecentHead();
		if (decent != nullptr)
			break;
    entry = entry->getParent();
  }

	if (decent != nullptr) {
		while (decent != nullptr) {
			if (entrySelector(*decent)) {
				return decent;
			}

			decent = decent->getParent();
		}
	}

	return nullptr;
}

Entry*
NameTree::findLongestPrefixMatch(const pit::Entry& pitEntry, const EntrySelector& entrySelector) const
{
  const Entry* nte = this->getEntry(pitEntry);
  BOOST_ASSERT(nte != nullptr);

  const Name& name = pitEntry.getName();
  size_t depth = std::min(name.size(), getMaxDepth());
  if (nte->getName().size() < pitEntry.getName().size()) {
    // PIT entry name either exceeds depth limit or ends with an implicit digest: go deeper
    for (size_t i = nte->getName().size() + 1; i <= depth; ++i) {
      const Entry* exact = this->findExactMatch(name, i);
      if (exact == nullptr) {
        break;
      }
      nte = exact;
    }
  }

  return this->findLongestPrefixMatch(*nte, entrySelector);
}

Entry*
NameTree::findLongestPrefixMatchDecent(const pit::Entry& pitEntry, const EntrySelector& entrySelector) const
{
  const Entry* nte = this->getEntry(pitEntry);
  BOOST_ASSERT(nte != nullptr);

  const Name& name = pitEntry.getName();
  size_t depth = std::min(name.size(), getMaxDepth());
  if (nte->getName().size() < pitEntry.getName().size()) {
    // PIT entry name either exceeds depth limit or ends with an implicit digest: go deeper
    for (size_t i = nte->getName().size() + 1; i <= depth; ++i) {
      const Entry* exact = this->findExactMatch(name, i);
      if (exact == nullptr) {
        break;
      }
      nte = exact;
    }
  }

	return this->findLongestPrefixMatchDecent(*nte, entrySelector);
}

boost::iterator_range<NameTree::const_iterator>
NameTree::findAllMatches(const Name& name, const EntrySelector& entrySelector) const
{
  // As we are using Name Prefix Hash Table, and the current LPM() is
  // implemented as starting from full name, and reduce the number of
  // components by 1 each time, we could use it here.
  // For trie-like design, it could be more efficient by walking down the
  // trie from the root node.

  Entry* entry = this->findLongestPrefixMatch(name, entrySelector);
  return {Iterator(make_shared<PrefixMatchImpl>(*this, entrySelector), entry), end()};
}

boost::iterator_range<NameTree::const_iterator>
NameTree::findAllMatches(const DecentName& name, const EntrySelector& entrySelector) const
{
  // As we are using Name Prefix Hash Table, and the current LPM() is
  // implemented as starting from full name, and reduce the number of
  // components by 1 each time, we could use it here.
  // For trie-like design, it could be more efficient by walking down the
  // trie from the root node.

  Entry* entry = this->findLongestPrefixMatch(name, entrySelector);
  return {Iterator(make_shared<PrefixMatchImpl>(*this, entrySelector), entry), end()};
}

boost::iterator_range<NameTree::const_iterator>
NameTree::findAllMatchesDecent(const Name& name, const EntrySelector& entrySelector) const
{
  // As we are using Name Prefix Hash Table, and the current LPM() is
  // implemented as starting from full name, and reduce the number of
  // components by 1 each time, we could use it here.
  // For trie-like design, it could be more efficient by walking down the
  // trie from the root node.

	Entry* entry = this->findLongestPrefixMatchDecent(name, entrySelector);
  return {Iterator(make_shared<PrefixMatchImpl>(*this, entrySelector), entry), end()};
}

boost::iterator_range<NameTree::const_iterator>
NameTree::fullEnumerate(const EntrySelector& entrySelector) const
{
  return {Iterator(make_shared<FullEnumerationImpl>(*this, entrySelector), nullptr), end()};
}

boost::iterator_range<NameTree::const_iterator>
NameTree::partialEnumerate(const Name& prefix,
                           const EntrySubTreeSelector& entrySubTreeSelector) const
{
  Entry* entry = this->findExactMatch(prefix);
  return {Iterator(make_shared<PartialEnumerationImpl>(*this, entrySubTreeSelector), entry), end()};
}

} // namespace name_tree
} // namespace nfd
