#ifndef CODIBOT_KEYTRACKERACCESSOR_H
#define CODIBOT_KEYTRACKERACCESSOR_H

#include <jcailloux/segcache/Cache.h>
#include <jcailloux/segcache/KeyTracker.h>

namespace jcailloux::segcache::test {
    template<typename K>
    struct KeyTrackerAccessor {
        static auto& getSegments(KeyTracker<K>& kt) {
            return kt.segments_;
        }
        static auto& getSegment(KeyTracker<K>& kt, size_t i) {
            return kt.segments_[i];
        }
        static auto& getCleanupBuffer(KeyTracker<K>& kt) {
            return kt.cleanup_buffer_;
        }
    };
};

#endif //CODIBOT_KEYTRACKERACCESSOR_H