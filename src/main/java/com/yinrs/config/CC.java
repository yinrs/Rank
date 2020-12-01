package com.yinrs.config;

import com.yinrs.cache.redis.RedisNode;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author ohun@live.cn
 */
public class CC {
    static Config cfg = load();

    static Config load() {
        Config config = ConfigFactory.load(CC.class.getClassLoader(), "reference.conf");//扫描加载所有可用的配置文件
        String custom_conf = "mp.conf";//加载自定义配置, 值来自jvm启动参数指定-Dmp.conf
        if (config.hasPath(custom_conf)) {
            File file = new File(config.getString(custom_conf));
            if (file.exists()) {
                Config custom = ConfigFactory.parseFile(file);
                config = custom.withFallback(config);
            }
        }
        return config;
    }

    public static class mp {
        public static Config cfg = CC.cfg.getObject("mp").toConfig();
        public static String log_dir = cfg.getString("log-dir");
        public static String log_level = cfg.getString("log-level");
        public static String log_conf_path = cfg.getString("log-conf-path");

        public static class core {
            static Config cfg = mp.cfg.getObject("core").toConfig();

            //int session_expired_time = (int) cfg.getDuration("session-expired-time").getSeconds();

            public static int max_heartbeat = (int) cfg.getDuration("max-heartbeat", TimeUnit.MILLISECONDS);

            public static long max_packet_size = cfg.getBytes("max-packet-size");

            public static int min_heartbeat = (int) cfg.getDuration("min-heartbeat", TimeUnit.MILLISECONDS);

            public static long compress_threshold = cfg.getBytes("compress-threshold");

            public static int max_hb_timeout_times = cfg.getInt("max-hb-timeout-times");

            public static String epoll_provider = cfg.getString("epoll-provider");

            public static boolean useNettyEpoll() {
                if (!"netty".equals(CC.mp.core.epoll_provider)) return false;
                String name = CC.cfg.getString("os.name").toLowerCase(Locale.UK).trim();
                return name.startsWith("linux");//只在linux下使用netty提供的epoll库
            }
        }

        public static class net {
            static Config cfg = mp.cfg.getObject("net").toConfig();

            public static int connect_server_port = cfg.getInt("connect-server-port");
            public static String connect_server_host = cfg.getString("connect-server-host");
            int gateway_server_port = cfg.getInt("gateway-server-port");
            int admin_server_port = cfg.getInt("admin-server-port");
            int gateway_client_port = cfg.getInt("gateway-client-port");

            public  static String gateway_server_net = cfg.getString("gateway-server-net");
            public  static String connect_server_net = cfg.getString("connect-server-net");
            String gateway_server_multicast = cfg.getString("gateway-server-multicast");
            String gateway_client_multicast = cfg.getString("gateway-client-multicast");
            static int ws_server_port = cfg.getInt("ws-server-port");
            String ws_path = cfg.getString("ws-path");
            int gateway_client_num = cfg.getInt("gateway-client-num");

            static boolean tcpGateway() {
                return "tcp".equals(gateway_server_net);
            }

            static boolean udpGateway() {
                return "udp".equals(gateway_server_net);
            }

            static boolean wsEnabled() {
                return ws_server_port > 0;
            }

            static boolean udtGateway() {
                return "udt".equals(gateway_server_net);
            }

            static boolean sctpGateway() {
                return "sctp".equals(gateway_server_net);
            }


            public static class public_ip_mapping {

                static Map<String, Object> mappings = net.cfg.getObject("public-host-mapping").unwrapped();

                static String getString(String localIp) {
                    return (String) mappings.get(localIp);
                }
            }

            /*public static class snd_buf {
                Config cfg = net.cfg.getObject("snd_buf").toConfig();
                int connect_server = (int) cfg.getMemorySize("connect-server").toBytes();
                int gateway_server = (int) cfg.getMemorySize("gateway-server").toBytes();
                int gateway_client = (int) cfg.getMemorySize("gateway-client").toBytes();
            }

            public static class rcv_buf {
                Config cfg = net.cfg.getObject("rcv_buf").toConfig();
                int connect_server = (int) cfg.getMemorySize("connect-server").toBytes();
                int gateway_server = (int) cfg.getMemorySize("gateway-server").toBytes();
                int gateway_client = (int) cfg.getMemorySize("gateway-client").toBytes();
            }

            public static class write_buffer_water_mark {
                Config cfg = net.cfg.getObject("write-buffer-water-mark").toConfig();
                int connect_server_low = (int) cfg.getMemorySize("connect-server-low").toBytes();
                int connect_server_high = (int) cfg.getMemorySize("connect-server-high").toBytes();
                int gateway_server_low = (int) cfg.getMemorySize("gateway-server-low").toBytes();
                int gateway_server_high = (int) cfg.getMemorySize("gateway-server-high").toBytes();
            }*/

            public static class traffic_shaping {
                static Config cfg = net.cfg.getObject("traffic-shaping").toConfig();

                public static class gateway_client {
                    static Config cfg = traffic_shaping.cfg.getObject("gateway-client").toConfig();
                    boolean enabled = cfg.getBoolean("enabled");
                    long check_interval = cfg.getDuration("check-interval", TimeUnit.MILLISECONDS);
                    long write_global_limit = cfg.getBytes("write-global-limit");
                    long read_global_limit = cfg.getBytes("read-global-limit");
                    long write_channel_limit = cfg.getBytes("write-channel-limit");
                    long read_channel_limit = cfg.getBytes("read-channel-limit");
                }

                public static class gateway_server {
                    Config cfg = traffic_shaping.cfg.getObject("gateway-server").toConfig();
                    boolean enabled = cfg.getBoolean("enabled");
                    long check_interval = cfg.getDuration("check-interval", TimeUnit.MILLISECONDS);
                    long write_global_limit = cfg.getBytes("write-global-limit");
                    long read_global_limit = cfg.getBytes("read-global-limit");
                    long write_channel_limit = cfg.getBytes("write-channel-limit");
                    long read_channel_limit = cfg.getBytes("read-channel-limit");
                }

                public static class connect_server {
                    Config cfg = traffic_shaping.cfg.getObject("connect-server").toConfig();
                    boolean enabled = cfg.getBoolean("enabled");
                    long check_interval = cfg.getDuration("check-interval", TimeUnit.MILLISECONDS);
                    long write_global_limit = cfg.getBytes("write-global-limit");
                    long read_global_limit = cfg.getBytes("read-global-limit");
                    long write_channel_limit = cfg.getBytes("write-channel-limit");
                    long read_channel_limit = cfg.getBytes("read-channel-limit");
                }
            }
        }

        public static class security {

            public static Config cfg = mp.cfg.getObject("security").toConfig();

            public static int aes_key_length = cfg.getInt("aes-key-length");

            public static String public_key = cfg.getString("public-key");

            public static String private_key = cfg.getString("private-key");

        }

        public static class kafka {
            public static Config cfg = mp.cfg.getObject("kafka").toConfig();
            public static String zk_list = cfg.getString("zk-list");
            public static String broken_list = cfg.getString("broken-list");
            public static String topic_gateway = cfg.getString("topic-push-gateway");
            //public static int topic_gateway_partition = cfg.getInt("customer-push-gateway-partition");
            static String p;
            public static int topic_gateway_partition = (p = System.getProperty("mp.kafka_partition")) == null ? 0
                    : Integer.parseInt(p);
        }

        public static class thread {

            static Config cfg = mp.cfg.getObject("thread").toConfig();

            public static class pool {

                static Config cfg = thread.cfg.getObject("pool").toConfig();

                public static int conn_work = cfg.getInt("conn-work");
                public static int http_work = cfg.getInt("http-work");
                public static int push_task = cfg.getInt("push-task");
                public static int push_client = cfg.getInt("push-client");
                public static int ack_timer = cfg.getInt("ack-timer");
                int gateway_server_work = cfg.getInt("gateway-server-work");
                int gateway_client_work = cfg.getInt("gateway-client-work");

                public static class event_bus {
                    public static Config cfg = pool.cfg.getObject("event-bus").toConfig();
                    public static int min = cfg.getInt("min");
                    public static int max = cfg.getInt("max");
                    public static int queue_size = cfg.getInt("queue-size");

                }

                public static class mq {
                    public static Config cfg = pool.cfg.getObject("mq").toConfig();
                    public static int min = cfg.getInt("min");
                    public static int max = cfg.getInt("max");
                    public static int queue_size = cfg.getInt("queue-size");
                }
            }
        }

        public static class zk {

            static Config cfg = mp.cfg.getObject("zk").toConfig();

            int sessionTimeoutMs = (int) cfg.getDuration("sessionTimeoutMs", TimeUnit.MILLISECONDS);

            String watch_path = cfg.getString("watch-path");

            int connectionTimeoutMs = (int) cfg.getDuration("connectionTimeoutMs", TimeUnit.MILLISECONDS);

            String namespace = cfg.getString("namespace");

            String digest = cfg.getString("digest");

            public static String server_address = cfg.getString("server-address");

            public static class retry {

                static Config cfg = zk.cfg.getObject("retry").toConfig();

                int maxRetries = cfg.getInt("maxRetries");

                int baseSleepTimeMs = (int) cfg.getDuration("baseSleepTimeMs", TimeUnit.MILLISECONDS);

                int maxSleepMs = (int) cfg.getDuration("maxSleepMs", TimeUnit.MILLISECONDS);
            }
        }

        public static class redis {
            static Config cfg = mp.cfg.getObject("redis").toConfig();

            boolean write_to_zk = cfg.getBoolean("write-to-zk");
            public static String password = cfg.getString("password");
            public static String clusterModel = cfg.getString("cluster-model");
            public static String mode = cfg.getString("cluster-model");
            public static String master = cfg.getString("sentinelMaster");

            public static List<RedisNode> nodes() {
                List<Object> list = cfg.getList("nodes").unwrapped();
                List<RedisNode> reList = new ArrayList<>(list.size());
                for (Object o : list) {
                    reList.add(RedisNode.from((String) o));
                }
                return reList;
            }

            public static boolean isCluster() {
                return "cluster".equals(clusterModel);
            }

            public static <T> T getPoolConfig(Class<T> clazz) {
                return ConfigBeanImpl.createInternal(cfg.getObject("config").toConfig(), clazz);
            }
        }

//        public static class http {
//
//            Config cfg = mp.cfg.getObject("http").toConfig();
//            boolean proxy_enabled = cfg.getBoolean("proxy-enabled");
//            int default_read_timeout = (int) cfg.getDuration("default-read-timeout", TimeUnit.MILLISECONDS);
//            int max_conn_per_host = cfg.getInt("max-conn-per-host");
//
//
//            long max_content_length = cfg.getBytes("max-content-length");
//
//            Map<String, List<DnsMapping>> dns_mapping = loadMapping();
//
//            static Map<String, List<DnsMapping>> loadMapping() {
//                Map<String, List<DnsMapping>> map = new HashMap<>();
//                cfg.getObject("dns-mapping").forEach((s, v) ->
//                        map.put(s, ConfigList.class.cast(v)
//                                .stream()
//                                .map(cv -> DnsMapping.parse((String) cv.unwrapped()))
//                                .collect(toCollection(ArrayList::new))
//                        )
//                );
//                return map;
//            }
//        }

        public static class push {

            static Config cfg = mp.cfg.getObject("push").toConfig();

            public static class flow_control {

                static Config cfg = push.cfg.getObject("flow-control").toConfig();
                public static boolean enabled = flow_control.cfg.getBoolean("enabled");

                public static class global {
                    public static Config cfg = flow_control.cfg.getObject("global").toConfig();
                    public static int limit = cfg.getNumber("limit").intValue();
                    public static int max = cfg.getInt("max");
                    public static int duration = (int) cfg.getDuration("duration", TimeUnit.MILLISECONDS);
                }

                public static class broadcast {
                    Config cfg = flow_control.cfg.getObject("broadcast").toConfig();
                    int limit = cfg.getInt("limit");
                    int max = cfg.getInt("max");
                    int duration = (int) cfg.getInt("duration");
                }
            }
        }

        public static class monitor {
            static Config cfg = mp.cfg.getObject("monitor").toConfig();
            public static String dump_dir = cfg.getString("dump-dir");
            boolean dump_stack = cfg.getBoolean("dump-stack");
            boolean print_log = cfg.getBoolean("print-log");
            long dump_period = cfg.getDuration("dump-period", TimeUnit.MINUTES);
            public static boolean profile_enabled = cfg.getBoolean("profile-enabled");
            public static long profile_slowly_duration = cfg.getDuration("profile-slowly-duration", TimeUnit.MILLISECONDS);
        }

        public static class spi {
            Config cfg = mp.cfg.getObject("spi").toConfig();
            String thread_pool_factory = cfg.getString("thread-pool-factory");
            String dns_mapping_manager = cfg.getString("dns-mapping-manager");
        }
    }
}
