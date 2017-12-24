package io.mycat.route.sequence.handler;

import io.mycat.config.loader.console.ZookeeperPath;
import io.mycat.config.loader.zkprocess.comm.ZkConfig;
import io.mycat.config.loader.zkprocess.comm.ZkParamCfg;
import io.mycat.route.util.PropertiesUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryNTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author tony
 * 1、JVM内 访问同一个table序列的线程共享该table在本地缓存的序列区间   |    IncrSequenceZKHandler则是利用ThreadLocal，每个线程一个table序列区间
 * 2、DistributedAtomicLong 实现原子增加值，先尝试乐观锁，失败则用悲观锁  | IncrSequenceZKHandler悲观锁
 * 3、该类解决了IncrSequenceZKHandler跳号严重的问题，因为是线程共享同一个table序列区间，
 * 不会像IncrSequenceZKHandler在线程销毁后，新线程需要重新在ZK获取序列空间，造成浪费序列号，而且频繁访问ZK影响性能。
 */
public class IncrAtomicSequenceZKHandler extends IncrSequenceHandler {

    protected static final Logger LOGGER = LoggerFactory.getLogger(IncrSequenceHandler.class);
    private final static String PATH = ZookeeperPath.ZK_SEPARATOR.getKey() + ZookeeperPath.FLOW_ZK_PATH_BASE.getKey()
            + ZookeeperPath.ZK_SEPARATOR.getKey()
            + io.mycat.config.loader.zkprocess.comm.ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_CLUSTERID)
            + ZookeeperPath.ZK_SEPARATOR.getKey() + ZookeeperPath.FLOW_ZK_PATH_SEQUENCE.getKey()
            + ZookeeperPath.ZK_SEPARATOR.getKey() + ZookeeperPath.FLOW_ZK_PATH_SEQUENCE_INCREMENT_SEQ.getKey();
    private final static String SEQ = "/seq";
    private final static IncrAtomicSequenceZKHandler instance = new IncrAtomicSequenceZKHandler();

    //TableSequence 多例存储器，每个Table一个序列生成器
    private static Map<String, TableSequence> tableSequenceMap = new ConcurrentHashMap<String, TableSequence>();

    private CuratorFramework client;


    public static IncrAtomicSequenceZKHandler getInstance() {
        return instance;
    }

    public void load() {
        Properties props = PropertiesUtil.loadProps(FILE_NAME);
        try {
            String zkAddress = ZkConfig.getInstance().getZkURL();
            this.client = CuratorFrameworkFactory.newClient(zkAddress, new ExponentialBackoffRetry(1000, 3));
            this.client.start();
            tableSequenceFactory(props);
        } catch (Exception e) {
            LOGGER.error("Error caught while initializing ZK:" + e.getCause());
        }
    }

    private synchronized void  tableSequenceFactory(Properties props){
        Enumeration<?> enu = props.propertyNames();
        while (enu.hasMoreElements()) {
            String key = (String) enu.nextElement();
            if (key.endsWith(KEY_MIN_NAME)) {
                String tableName = key.substring(0, key.indexOf(KEY_MIN_NAME));
                if(!tableSequenceMap.containsKey(tableName)){
                    Map<String,String> paraValMap = new ConcurrentHashMap<String,String>();
                    paraValMap.put(tableName + KEY_MIN_NAME, props.getProperty(key));
                    paraValMap.put(tableName + KEY_MAX_NAME, props.getProperty(tableName + KEY_MAX_NAME));
                    paraValMap.put(tableName + KEY_CUR_NAME, props.getProperty(tableName + KEY_CUR_NAME));
                    tableSequenceMap.put(tableName, new TableSequence(tableName,paraValMap));
                }

            }
        }
    }


    /**
     * 大部分线程通过该方法获取当前值
     * @param prefixName
     * @return
     */
    @Override
    public Map<String, String> getParaValMap(String prefixName) {
        return this.tableSequenceMap.get(prefixName).paraValMap;
    }

    /**
     * 所有线程在本地递增值后，调用该方法存入本地Map
     * @param prefixName
     * @param val
     * @return
     */
    @Override
    public Boolean updateCURIDVal(String prefixName, Long val) {

        return this.tableSequenceMap.get(prefixName).updateCURIDVal(val);
    }

    /**
     * 发现 (nextId > maxId) 的线程，调用该方法请求新的序列区间
     * @param prefixName
     * @return
     */
    @Override
    public Boolean fetchNextPeriod(String prefixName) {
        return this.tableSequenceMap.get(prefixName).fetchNextPeriod();
    }

    /**
     * 每个Table序列一个实例，由 @link IncrAtomicSequenceZKHandler#initialize 保证。
     * 线程安全，DruidSequenceHandler为每个table序列号设置了排他锁，也就是说同一个table序列号的操作是线程安全的。
     * @link io.mycat.route.parser.druid.DruidSequenceHandler#getExecuteSql(SessionSQLPair pair, String charset)
     */
    private class TableSequence{
        final String tableName;
        final String keyMini;
        final String keyMax;
        final String keyCurr;
        final Map<String, String> paraValMap;
        final DistributedAtomicLong count;
        TableSequence(String tableName, Map<String ,String> paraValMap){
            this.tableName = tableName;
            this.keyMini = this.tableName + KEY_MIN_NAME;
            this.keyMax = this.tableName + KEY_MAX_NAME;
            this.keyCurr = this.tableName + KEY_CUR_NAME;
            this.paraValMap = paraValMap;
            //ZK
            String seqPath = PATH + ZookeeperPath.ZK_SEPARATOR.getKey() + this.tableName + SEQ;
            this.count = new DistributedAtomicLong(client, seqPath, new RetryNTimes(3, 10));
            init();
        }

        /**
         * 如果ZK的Table序列值已存在，则更新本地Map，否则用keymini值设置为ZK的初始值，再更新本地Map。
         */
        private void init(){
            try {
                // 初始化 ZK 的Table序列值，用KeyMini的值
                if (this.count.get().postValue() == 0) {

                    String val = this.paraValMap.get(this.keyMini);
                    this.count.initialize(Long.valueOf(val));

                }
            } catch (Exception e) {
                LOGGER.error("Error caught while initializing DistributedAtomicLong:" + e.getCause());
            }


            fetchNextPeriod();

        }

        /**
         * 获取下一个区间的值，并更新本地 paraValMap
         * @return
         */
        Boolean fetchNextPeriod() {
            long period = Long.parseLong(this.paraValMap.get(this.keyMax))
                    - Long.parseLong(this.paraValMap.get(this.keyMini));
            try {
                // ZK 修改前的Table序列值
                long now = this.count.get().postValue().longValue();
                // ZK 增加一个区间数字后Table序列的值
                AtomicValue<Long> maxAtomicValue = this.count.add(period + 1);
                // 设置Map
                this.paraValMap.put(this.keyMax,String.valueOf(maxAtomicValue.postValue()));
                this.paraValMap.put(this.keyMini,String.valueOf(now+1));
                this.paraValMap.put(this.keyCurr,String.valueOf(now));
            }catch (Exception e){
                LOGGER.error("Error caught while fetchNextPeriod:" + e.getCause());
                return false;
            }
            return true;
        }

        /**
         * 更新本地Map的 curr 值
         * @param val
         * @return
         */
        public Boolean updateCURIDVal(Long val){
            this.paraValMap.put(this.keyCurr, String.valueOf(val));
            return true;
        }




    }




}
