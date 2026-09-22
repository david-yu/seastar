/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2026 ScyllaDB Ltd.
 */

#pragma once

#include <cstdlib>

#include <boost/test/unit_test.hpp>

#include <seastar/core/reactor.hh>
#include <seastar/net/api.hh>

namespace seastar::testing {

// Tests that need IPv6 skip on hosts without it. With SEASTAR_TEST_REQUIRE_IPV6
// set in the environment they fail instead, so an environment that is meant
// to have IPv6 (CI, an IPv6-only container) notices when they stop running.
inline bool ipv6_available_or_skip() {
    if (engine().net().supports_ipv6()) {
        return true;
    }
    if (std::getenv("SEASTAR_TEST_REQUIRE_IPV6")) {
        BOOST_FAIL("IPv6 is required (SEASTAR_TEST_REQUIRE_IPV6) but the network stack reports no support");
    }
    BOOST_TEST_MESSAGE("No ipv6 support, skipping test");
    return false;
}

}
