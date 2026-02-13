#ifndef CODIBOT_CACHEACCESSOR_H
#define CODIBOT_CACHEACCESSOR_H

#include <jcailloux/segcache/Cache.h>
#include <jcailloux/segcache/KeyTracker.h>

namespace jcailloux::segcache::test {
    template<typename K, typename V, typename Metadata>
    struct CacheAccessor {
        static auto& getKeyTracker(Cache<K, V, Metadata>& cache) {
            return cache.key_tracker_;
        }
    };
}

#endif //CODIBOT_CACHEACCESSOR_H