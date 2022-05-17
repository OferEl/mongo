from deepdiff import DeepDiff as df
import redshift_connector as rsc
import boto3 as boto3
import sys
import datetime
import asyncio
import pandas as pd

dc_events ={
    "NetworkPing": "network|ping",
    "TrafficSwgUrlBlock": "traffic|swg|url_block"
}
"""
dc_events ={
    "TrafficSwgUrlBlock": "traffic|swg|url_block"
        "NetworkConnectFailure": "network|connect_failure",
        "SystemAwake": "system|awake",
        "SupportUserGuidesEnter": "support|user_guides|enter",
        "SystemSleep": "system|sleep",
        "TrafficSwgUrlBlock": "traffic|swg|url_block"
        "WorkspaceLaunch": "workspace|launch",
        "TrafficSwgUrlWarn": "traffic|swg|url_warn",
        "SettingsGeneralEnableStartMinimized": "settings|general|enable_start_minimized",
        "SettingsNetworkDisableWifiSecurity": "settings|network|disable_wifi_security",
        "SettingsNetworkTrustedNetworkAdded": "settings|network|trusted_network_added"ls
        "SettingsNetworkEnableAlwaysOn": "settings|network|enable_always_on",
        "SettingsNetworkDisableAlwaysOn": "settings|network|disable_always_on",
        "SdpLogin" :"sdp|login",
        "NetworkConnectAppInitiated": "network|connect_app_initiated",
        "DeviceRegister": "device|register",
        "NetworkPing": "network|ping",
        "DeviceRegisterFailed": "device|register_failed",
        "NetworkConnectSuccess": "network|connect|success",
        "SdpLoginFailed": "sdp|login_failed",
        "NetworkDisconnectAppInitiated": "network|disconnect_app_initiated",
        "SdpLoginSuccess": "sdp|login_success",
        "SdpSocketDropped": "sdp|socket|dropped",
        "SettingsGeneralDisableAutomaticUpdates": "settings|general|disable_automatic_updates",
        "SettingsGeneralDisableSendCrashReport": "settings|general|disable_send_crash_report",
        "DeviceDpcFailed":"device|dpc_failed",
        "DeviceRegisterSuccess":"device|register_success",
        "NetworkChooseNetwork":"network|choose_network",
        "NetworkConnectUserInitiated":"network|connect_user_initiated",
        "NetworkDisconnectUserInitiated":"network|disconnect_user_initiated",
        "SettingsGeneralCheckForUpdates":"settings|general|check_for_updates",
        "SettingsGeneralDisableAutoLaunch":"settings|general|disable_auto_launch",
        "SettingsGeneralDisableConnectOnLaunch":"settings|general|enable_connect_on_launch",
        "SettingsGeneralDisableNotifications":"settings|general|disable_notifications",
        "SettingsGeneralDisableSendAnalyticsReport":"settings|general|disable_send_analytics_report",
        "SettingsGeneralDisableStartMinimized":"settings|general|disable_start_minimized",
        "SettingsGeneralEnableAutoLaunch":"settings|general|enable_auto_launch",
        "SettingsGeneralEnableAutomaticUpdates":"settings|general|enable_automatic_updates",
        "SettingsGeneralEnableConnectOnLaunch":"settings|general|enable_connect_on_launch",
        "SettingsGeneralEnableNotifications":"settings|general|enable_notifications",
        "SettingsGeneralEnableSendAnalyticsReport":"settings|general|enable_send_analytics_report",
        "SettingsGeneralEnableSendCrashReport":"settings|general|enable_send_crash_report",
        "SettingsNetworkDisableTrustedNetworks":"settings|network|disable_trusted_networks",
        "SettingsNetworkEnableTrustedNetworks":"settings|network|enable_trusted_networks",
        "SettingsNetworkEnableWifiSecurity":"settings|network|enable_wifi_security",
        "SettingsNetworkTrustedNetworkRemoved":"settings|network|trusted_network_removed",
        "SettingsProtocolsVpnChanged":"settings|protocols|vpn_changed",
        "SupportChatStart":"support|chat|start",
        "SupportSupportRunTroubleshootingCompleted":"support|support|run_troubleshooting_completed",
        "SupportSupportRunTroubleshootingFailed":"support|support|run_troubleshooting_failed",
        "SupportSupportRunTroubleshootingStart":"support|support|run_troubleshooting_start",
        "WorkspaceLogout":"workspace|logout",
        "WorkspaceWebAuthSuccess":"workspace|web_auth_success"
     }
"""
n = len(sys.argv)
argv = sys.argv[1:3]

for idx, arg in enumerate(sys.argv):
    if idx==0:
       int_hr=12
    if idx==1:
        try:
            dt = datetime.datetime.strptime(arg, '%d-%m-%y').date()
        except ValueError as ve:
            raise ValueError(ve)
    if idx == 2:
        try:
            int_hr = int(arg)
            if int_hr > 24:
                print('must be lass then 24, default is 12pm')
                int_hr=12
        except ValueError as ve:
            raise ValueError(ve)
    if idx == 3:
      try:
        s3_event_name = arg
      except ValueError as ve:
        raise ValueError(ve)

str_date_filter = str(dt)
l = len(str(int_hr))
if len(str(int_hr)) == 2 :
    hr=str(int_hr)
else:
    hr = '0'+str(int_hr)

bucket_name = "p81-yarkon-data-staging"

rsconn = rsc.connect(
    host='snowplow-com-safervpn-main-red-resredshiftcluster-j4nqz5chy6qd.czqepfubfdjr.us-east-2.redshift.amazonaws.com',
    port=5439,
    database='snowplow',
    user='data_viewer',
    password='DEF1987leppard?!'
)
s3_client = boto3.client("s3")

async def s3_rds(name):
    #---redshift
    filter_from_time = datetime.datetime.combine(dt, datetime.time(int_hr))
    filter_to_time = datetime.datetime.combine(dt, datetime.time(int_hr + 1))
    print(str(filter_from_time)+  '  '+ str(filter_to_time) )
    dict_csv = {}
    for k,v in dc_events.items() :
        rscursor  = rsconn.cursor()
        rds_event_name = v
        s3_event_name = k

        cur = rscursor.execute("select count(distinct event_id) from lake.agent_events t where t.event_name =%s and t.server_tstamp >= %s and t.server_tstamp <= %s ;",(rds_event_name,filter_from_time,filter_to_time))
        row = cur.fetchone()
        while row:
            rds = (row)
            rds_count = rds[0]
            row = rscursor.fetchone()

        # ---s3
        all_count = 0
        file_path = "agents/event=" + s3_event_name + "/dt=" + str_date_filter + "-" + hr + "/"
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=file_path)
        files = response.get("Contents")
        if isinstance(files, list) and len(files) > 0:
            for file in files:
                resp = s3_client.select_object_content(
                        Bucket=bucket_name,
                        Key=file['Key'],
                        ExpressionType='SQL',
                        Expression="SELECT count(*) FROM s3object s",
                        InputSerialization={"JSON": {"Type": "Document"}},
                        OutputSerialization={'JSON': {}},
                )

                for event in resp['Payload']:
                    if 'Records' in event:
                        records = event['Records']['Payload'].decode('utf-8')
                        srt = records.find(':') + 1
                        end = records.rfind('}')
                    try:
                        i = int(records[srt:end])
                    except ValueError as ve:
                        raise ValueError(ve)
                all_count = all_count + i


            diff= rds_count - all_count
            csv_list = (rds_count,all_count,diff )
            dict_csv[k] = csv_list

        else:
            diff = rds_count - all_count
            csv_list = (rds_count, 0, diff)
            dict_csv[k]=csv_list

    print(' EVENTNAME   RDS  S3  DIFF ')
    for k,v in dict_csv.items() :
        tx =  str(k) + ', ' + str(v).replace('(','').replace(')','')
        print (tx)


async def main():
      await s3_rds('start')
      rsconn.close()

if __name__ == "__main__":
    asyncio.run(main())


