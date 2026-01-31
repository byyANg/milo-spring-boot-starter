package com.kangaroohy.milo.runner.subscription;

import com.kangaroohy.milo.utils.CustomUtil;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.subscriptions.MonitoredItemSynchronizationException;
import org.eclipse.milo.opcua.sdk.client.subscriptions.OpcUaMonitoredItem;
import org.eclipse.milo.opcua.sdk.client.subscriptions.OpcUaSubscription;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * 类 SubscriptionRunner 功能描述：
 *
 * @author kangaroo hy
 * @version 0.0.1
 * @date 2022/01/01 23:49
 */
@Slf4j
public class SubscriptionRunner {
    /**
     * 点位list
     */
    private final List<String> identifiers;

    private final double samplingInterval;

    public SubscriptionRunner(List<String> identifiers) {
        this.identifiers = identifiers;
        this.samplingInterval = 1000.0D;
    }

    public SubscriptionRunner(List<String> identifiers, double samplingInterval) {
        this.identifiers = identifiers;
        this.samplingInterval = samplingInterval;
    }

    public void run(OpcUaClient opcUaClient, SubscriptionCallback callback) {

        final CountDownLatch downLatch = new CountDownLatch(1);

        //处理订阅逻辑
        handler(opcUaClient, callback);

        try {
            //持续监听
            downLatch.await();
        } catch (Exception e) {
            log.error("订阅时出现了异常：{}", e.getMessage(), e);
        }
    }

    private void handler(OpcUaClient opcUaClient, SubscriptionCallback callback) {
        try {
            //创建订阅
            OpcUaSubscription subscription = new OpcUaSubscription(opcUaClient);

            // 设置订阅监听器
            subscription.setSubscriptionListener(new OpcUaSubscription.SubscriptionListener() {
                @Override
                public void onDataReceived(OpcUaSubscription subscription,
                                         List<OpcUaMonitoredItem> items,
                                         List<DataValue> values) {
                    for (int i = 0; i < items.size(); i++) {
                        callback.onSubscribe(items.get(i), values.get(i));
                    }
                }
            });

            // 创建订阅
            subscription.create();

            // 创建监控项
            for (String identifier : identifiers) {
                NodeId nodeId = CustomUtil.parseNodeId(identifier);
                var monitoredItem = OpcUaMonitoredItem.newDataItem(nodeId);

                // 设置采样间隔
                monitoredItem.setSamplingInterval(samplingInterval);

                // 设置队列大小
                monitoredItem.setQueueSize(UInteger.valueOf(10));

                // 添加到订阅
                subscription.addMonitoredItem(monitoredItem);
            }

            // 同步监控项到服务器
            try {
                subscription.synchronizeMonitoredItems();
            } catch (MonitoredItemSynchronizationException e) {
                log.error("监控项同步失败: {}", e.getMessage(), e);
                e.getCreateResults().forEach(result -> {
                    if (result.serviceResult().isBad()) {
                        log.error("创建监控项失败: {}, status: {}",
                            result.monitoredItem().getReadValueId().getNodeId(),
                            result.serviceResult());
                    }
                });
            }
        } catch (Exception e) {
            log.error("订阅时出现了异常：{}", e.getMessage(), e);
        }
    }
}
