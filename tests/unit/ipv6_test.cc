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
 * Copyright (C) 2019 Cloudius Systems, Ltd.
 */

#include <seastar/testing/test_case.hh>
#include <seastar/net/api.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/log.hh>

#include "ipv6_support.hh"

using namespace seastar;

static bool check_ipv6_support() {
    return seastar::testing::ipv6_available_or_skip();
}

SEASTAR_TEST_CASE(udp_packet_test) {
    if (!check_ipv6_support()) {
        return make_ready_future<>();
    }

    auto sc = make_bound_datagram_channel(ipv6_addr{"::1"});

    BOOST_REQUIRE(sc.local_address().addr().is_ipv6());

    auto cc = make_bound_datagram_channel(ipv6_addr{"::1"});

    auto f1 = cc.send(sc.local_address(), "apa");

    return f1.then([cc = std::move(cc), sc = std::move(sc)]() mutable {
        auto src = cc.local_address();
        cc.close();
        auto f2 = sc.receive();

        return f2.then([sc = std::move(sc), src](auto pkt) mutable {
            auto a = sc.local_address();
            sc.close();
            BOOST_REQUIRE_EQUAL(src, pkt.get_src());
            BOOST_REQUIRE_EQUAL(pkt.get_src().length(), sizeof(::sockaddr_in6));
            BOOST_REQUIRE_EQUAL(a, pkt.get_dst());
        });
    });
}

// The destination a datagram arrived on is only interesting for a wildcard
// socket, and it is only known through the pktinfo control message.
static future<> check_datagram_dst(socket_address wildcard, net::inet_address peer, socklen_t addr_len) {
    auto server = make_bound_datagram_channel(wildcard);
    auto client = make_bound_datagram_channel(socket_address(peer, 0));
    co_await client.send(socket_address(peer, server.local_address().port()), "apa");
    auto pkt = co_await server.receive();
    BOOST_REQUIRE_EQUAL(pkt.get_src(), client.local_address());
    BOOST_REQUIRE_EQUAL(pkt.get_src().length(), addr_len);
    BOOST_REQUIRE_EQUAL(pkt.get_dst().addr(), peer);
    BOOST_REQUIRE_EQUAL(pkt.get_dst().port(), server.local_address().port());
    client.close();
    server.close();
}

SEASTAR_TEST_CASE(udp_wildcard_dst_ipv6_test) {
    if (!check_ipv6_support()) {
        co_return;
    }
    co_await check_datagram_dst(ipv6_addr{"::", 0}, net::inet_address("::1"), sizeof(::sockaddr_in6));
}

SEASTAR_TEST_CASE(udp_wildcard_dst_ipv4_test) {
    co_await check_datagram_dst(ipv4_addr{"0.0.0.0", 0}, net::inet_address("127.0.0.1"), sizeof(::sockaddr_in));
}

SEASTAR_TEST_CASE(tcp_packet_test) {
    if (!check_ipv6_support()) {
        return make_ready_future<>();
    }

    return async([] {
        auto sc = server_socket(engine().net().listen(ipv6_addr{"::1"}, {}));
        auto la = sc.local_address();

        BOOST_REQUIRE(la.addr().is_ipv6());

        auto cc = connect(la).get();
        auto lc = std::move(sc.accept().get().connection);

        auto strm = cc.output();
        strm.write("los lobos").get();
        strm.flush().get();

        auto in = lc.input();

        using consumption_result_type = typename input_stream<char>::consumption_result_type;
        using stop_consuming_type = typename consumption_result_type::stop_consuming_type;
        using tmp_buf = stop_consuming_type::tmp_buf;

        in.consume([](tmp_buf buf) {
            return make_ready_future<consumption_result_type>(stop_consuming<char>({}));
        }).get();

        strm.close().get();
        in.close().get();
        sc.abort_accept();
    });
}

// A wildcard IPv6 listener decides for itself whether IPv4 clients may reach it,
// instead of inheriting the host's net.ipv6.bindv6only default.
SEASTAR_TEST_CASE(dual_stack_listen_test) {
    if (!check_ipv6_support()) {
        co_return;
    }
    for (bool ipv6_only : {false, true}) {
        listen_options lo;
        lo.reuse_address = true;
        lo.ipv6_only = ipv6_only;
        auto ss = server_socket(engine().net().listen(ipv6_addr{"::", 0}, lo));
        auto accepted = ss.accept();
        std::optional<connected_socket> cs;
        std::exception_ptr refused;
        try {
            cs = co_await connect(ipv4_addr("127.0.0.1", ss.local_address().port()));
        } catch (...) {
            refused = std::current_exception();
        }
        if (cs) {
            BOOST_REQUIRE_MESSAGE(!ipv6_only, "an IPv4 client reached an IPV6_V6ONLY listener");
            auto ar = co_await std::move(accepted);
            // the kernel hands the IPv4 peer over as ::ffff:127.0.0.1
            auto& peer = ar.remote_address.as_posix_sockaddr_in6();
            BOOST_REQUIRE_EQUAL(ar.remote_address.family(), AF_INET6);
            BOOST_REQUIRE(IN6_IS_ADDR_V4MAPPED(&peer.sin6_addr));
            cs->shutdown_output();
            ar.connection.shutdown_output();
        } else {
            BOOST_REQUIRE_MESSAGE(ipv6_only, "IPv4 client refused by a dual-stack listener");
            try {
                std::rethrow_exception(refused);
            } catch (const std::system_error& e) {
                BOOST_REQUIRE_EQUAL(e.code().value(), ECONNREFUSED);
            }
            ss.abort_accept();
            co_await std::move(accepted).then_wrapped([](future<accept_result> f) {
                f.ignore_ready_future();
            });
        }
    }
}

// listen() with no address at all listens for both families where it can,
// instead of quietly picking IPv4 and being unreachable on an IPv6-only host.
SEASTAR_TEST_CASE(listen_unspecified_address_test) {
    if (!check_ipv6_support()) {
        co_return;
    }
    auto ss = server_socket(engine().net().listen(socket_address(), {}));
    BOOST_REQUIRE_EQUAL(ss.local_address().family(), AF_INET6);
    BOOST_REQUIRE(ss.local_address().addr().is_addr_any());

    auto accepted = ss.accept();
    auto cs = co_await connect(ipv4_addr("127.0.0.1", ss.local_address().port()));
    auto ar = co_await std::move(accepted);
    // an IPv4 peer arrives, and (per the v4-mapped normalisation) reads as IPv4
    BOOST_REQUIRE_EQUAL(ar.remote_address.unmapped().addr(), net::inet_address("127.0.0.1"));
    cs.shutdown_output();
    ar.connection.shutdown_output();
}

// The IPv6 address type has the same value-type toolkit as the IPv4 one.
SEASTAR_TEST_CASE(ipv6_addr_value_type_test) {
    const ipv6_addr a("2001:db8::1", 9092);
    const ipv6_addr same("2001:db8::1", 9092);
    const ipv6_addr other_port("2001:db8::1", 9093);
    const ipv6_addr other_addr("2001:db8::2", 9092);

    BOOST_REQUIRE(a == same);
    BOOST_REQUIRE(!(a == other_port));
    BOOST_REQUIRE(!(a == other_addr));
    BOOST_REQUIRE_EQUAL(std::hash<ipv6_addr>()(a), std::hash<ipv6_addr>()(same));
    BOOST_REQUIRE_NE(std::hash<ipv6_addr>()(a), std::hash<ipv6_addr>()(other_port));

    BOOST_REQUIRE_EQUAL(make_ipv6_address(a), socket_address(a));
    BOOST_REQUIRE_EQUAL(fmt::to_string(make_ipv6_address(a)), "[2001:db8::1]:9092");

    // wildcard() says which family it means, unlike socket_address(port)
    BOOST_REQUIRE_EQUAL(socket_address::wildcard(AF_INET6, 9092).family(), AF_INET6);
    BOOST_REQUIRE_EQUAL(fmt::to_string(socket_address::wildcard(AF_INET6, 9092)), "[::]:9092");
    BOOST_REQUIRE_EQUAL(fmt::to_string(socket_address::wildcard(AF_INET, 9092)), "0.0.0.0:9092");
    BOOST_REQUIRE_EQUAL(socket_address::wildcard(AF_INET, 9092), socket_address(ipv4_addr(9092)));
    BOOST_REQUIRE(socket_address::wildcard(AF_INET6).is_wildcard());
    BOOST_REQUIRE(socket_address::wildcard(AF_INET).is_wildcard());
    BOOST_REQUIRE(socket_address::wildcard(AF_UNSPEC).is_unspecified());
    return make_ready_future();
}

SEASTAR_TEST_CASE(ipv6_equal_test) {
    const uint16_t port{8080};
    const uint16_t port2{8088};

    const std::string str_addr1{"abcd:fedc:ba98:7654:3210:0123:4567:89ab"};
    const std::string str_addr2{"0123:4567:89ab:cdef:3210:0123:4567:89ab"};
    const std::string str_addr3{"abcd:fedc:ba98:7654:3210:0123:4567:8900"};

    socket_address sock_addr1(ipv6_addr(str_addr1, port));
    socket_address sock_addr2(ipv6_addr(str_addr2, port));
    socket_address sock_addr3(ipv6_addr(str_addr1, port));

    socket_address sock_addr4(ipv6_addr(str_addr3, port));
    socket_address sock_addr5(ipv6_addr(str_addr1, port2));

    BOOST_CHECK_NE(sock_addr1, sock_addr2);
    BOOST_CHECK_EQUAL(sock_addr1, sock_addr3);
    BOOST_CHECK_NE(sock_addr1, sock_addr4);
    BOOST_CHECK_NE(sock_addr1, sock_addr5);

    return make_ready_future();
}

