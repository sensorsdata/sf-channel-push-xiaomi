/*
 * Copyright 2019 Sensors Data Co., Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.sensorsdata.focus.channel;

import com.sensorsdata.focus.channel.ChannelClient;
import com.sensorsdata.focus.channel.ChannelConfig;
import com.sensorsdata.focus.channel.annotation.SfChannelClient;
import com.sensorsdata.focus.channel.entry.LandingType;
import com.sensorsdata.focus.channel.entry.MessagingTask;
import com.sensorsdata.focus.channel.entry.PushTask;
import com.sensorsdata.focus.channel.push.PushTaskUtils;

import com.xiaomi.push.sdk.ErrorCode;
import com.xiaomi.xmpush.server.Constants;
import com.xiaomi.xmpush.server.Message;
import com.xiaomi.xmpush.server.Result;
import com.xiaomi.xmpush.server.Sender;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@SfChannelClient(version = "v0.1.1", desc = "SF 小米推送客户端")
@Slf4j
public class XiaomiClient extends ChannelClient {

  // 每次批量发送请求最多包含多少推送 ID
  // 小米推送一次请求中最多推送客户端 ID 数量，官方规定 1000 个，这里默认 800
  private static final int BATCH_SIZE = 800;

  private static final String STR_ANDROID = "android";
  private static final String STR_IOS = "ios";
  private static final String STR_SF_DATA = "sf_data";

  private Sender iosSender;
  private Sender androidSender;

  @Override
  public void initChannelClient(ChannelConfig channelConfig) {
    XiaomiChannelConfig xiaomiChannelConfig = (XiaomiChannelConfig) channelConfig;

    Constants.useOfficial();
    androidSender = new Sender(xiaomiChannelConfig.getAndroidAppSecret());
    iosSender = new Sender(xiaomiChannelConfig.getIosAppSecret());
  }

  @Override
  public void send(List<MessagingTask> messagingTasks) throws Exception {
    if (CollectionUtils.isEmpty(messagingTasks)) {
      return;
    }
    // 先区分 Android 和 iOS
    List<MessagingTask> androidMessagingTasks = new ArrayList<>();
    List<MessagingTask> iosMessagingTasks = new ArrayList<>();
    for (MessagingTask messagingTask : messagingTasks) {
      String clientId = messagingTask.getPushTask().getClientId();
      if (clientId.startsWith(STR_IOS)) {
        iosMessagingTasks.add(messagingTask);
      } else if (clientId.startsWith(STR_ANDROID)) {
        androidMessagingTasks.add(messagingTask);
      } else {
        log.warn("no platform prefix. [task='{}']", messagingTask);
      }
    }
    flushByPlatformSender(androidMessagingTasks, true);
    flushByPlatformSender(iosMessagingTasks, false);
  }

  private void flushByPlatformSender(List<MessagingTask> messagingTasks,
      boolean isAndroidTasks) {
    Sender sender = iosSender;
    if (isAndroidTasks) {
      sender = androidSender;
    }
    Collection<List<MessagingTask>> taskGroups = PushTaskUtils.groupByTaskContent(messagingTasks, BATCH_SIZE);
    for (List<MessagingTask> taskList : taskGroups) {
      List<String> regIdList = new ArrayList<>();
      for (MessagingTask messagingTask : taskList) {
        String clientId = messagingTask.getPushTask().getClientId();
        if (StringUtils.isNotBlank(clientId)) {
          if (isAndroidTasks) {
            regIdList.add(clientId.substring(STR_ANDROID.length() + 1));
          } else {
            regIdList.add(clientId.substring(STR_IOS.length() + 1));
          }
        }
      }
      if (CollectionUtils.isNotEmpty(regIdList)) {
        Message message;
        if (isAndroidTasks) {
          message = constructAndroidMessage(taskList);
        } else {
          message = constructIosMessage(taskList);
        }
        String failReason = null;
        try {
          log.debug("send request. [message='{}', regIdList='{}']", message, regIdList);
          Result result = sender.send(message, regIdList, 3);
          log.debug("send finished. [result='{}']", result);
          if (!ErrorCode.Success.equals(result.getErrorCode())) {
            failReason = result.getReason();
          }
        } catch (Exception e) {
          log.warn("push message with exception. [tasks='{}']", taskList);
          log.warn("exception detail:", e);
          failReason = ExceptionUtils.getMessage(e);
        }
        for (MessagingTask messagingTask : taskList) {
          messagingTask.setFailReason(failReason);
          messagingTask.setSuccess(failReason == null);
        }
      }
    }
  }

  // 构造 Android 数据
  private Message constructAndroidMessage(List<MessagingTask> taskList) {
    MessagingTask messagingTask = taskList.get(0);
    PushTask pushTask = messagingTask.getPushTask();
    String title = pushTask.getMsgTitle();
    String description = pushTask.getMsgContent();
    Message.Builder messageBuilder = new Message.Builder()
        .title(title)
        .description(description);
    /*
     * 设置 extra.notify_effect 的值以得到不同的预定义点击行为。
     * “1″：通知栏点击后打开app的Launcher Activity。
     * “2″：通知栏点击后打开app的任一Activity（开发者还需要传入extra.intent_uri）。
     * “3″：通知栏点击后打开网页（开发者还需要传入extra.web_uri）。
     */
    if (pushTask.getLandingType().equals(LandingType.OPEN_APP)) {
      messageBuilder.extra(Constants.EXTRA_PARAM_NOTIFY_EFFECT, Constants.NOTIFY_LAUNCHER_ACTIVITY);
    } else if (pushTask.getLandingType().equals(LandingType.LINK)) {
      messageBuilder.extra(Constants.EXTRA_PARAM_NOTIFY_EFFECT, Constants.NOTIFY_WEB);
      messageBuilder.extra(Constants.EXTRA_PARAM_WEB_URI, pushTask.getLinkUrl());
    }
    Map<String, String> extraMap = new LinkedHashMap<>();
    extraMap.put(STR_SF_DATA, pushTask.getSfData());
    if (MapUtils.isNotEmpty(extraMap)) {
      for (Map.Entry<String, String> entry : extraMap.entrySet()) {
        if (StringUtils.isNotBlank(entry.getKey()) && entry.getValue() != null) {
          messageBuilder.extra(entry.getKey(), entry.getValue());
        }
      }
    }
    return messageBuilder.build();
  }

  // 构造 iOS 数据
  private Message constructIosMessage(List<MessagingTask> taskList) {
    MessagingTask messagingTask = taskList.get(0);
    PushTask pushTask = messagingTask.getPushTask();
    String title = pushTask.getMsgTitle();
    String description = pushTask.getMsgContent();
    Message.IOSBuilder messageBuilder = new Message.IOSBuilder()
        .title(title)
        .body(description);
    /*
     * 设置 extra.notify_effect 的值以得到不同的预定义点击行为。
     * “1″：通知栏点击后打开app的Launcher Activity。
     * “2″：通知栏点击后打开app的任一Activity（开发者还需要传入extra.intent_uri）。
     * “3″：通知栏点击后打开网页（开发者还需要传入extra.web_uri）。
     */
    if (pushTask.getLandingType().equals(LandingType.OPEN_APP)) {
      messageBuilder.extra(Constants.EXTRA_PARAM_NOTIFY_EFFECT, Constants.NOTIFY_LAUNCHER_ACTIVITY);
    } else if (pushTask.getLandingType().equals(LandingType.LINK)) {
      messageBuilder.extra(Constants.EXTRA_PARAM_NOTIFY_EFFECT, Constants.NOTIFY_WEB);
      messageBuilder.extra(Constants.EXTRA_PARAM_WEB_URI, pushTask.getLinkUrl());
    }
    Map<String, String> extraMap = new LinkedHashMap<>();
    extraMap.put(STR_SF_DATA, pushTask.getSfData());
    if (MapUtils.isNotEmpty(extraMap)) {
      for (Map.Entry<String, String> entry : extraMap.entrySet()) {
        if (StringUtils.isNotBlank(entry.getKey()) && entry.getValue() != null) {
          messageBuilder.extra(entry.getKey(), entry.getValue());
        }
      }
    }
    return messageBuilder.build();
  }
}
