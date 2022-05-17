import redshift_connector as rsc
import boto3 as boto3
import sys
import datetime
import asyncio


dc_events ={
     "NetworkConnectAppInitiated": "network|connect_app_initiated",
     "DeviceRegister": "device|register",
     "NetworkConnectFailure": "network|connect_failure",
     "NetworkPing": "network|ping" ,
     "DeviceRegisterFailed": "device|register_failed",
     "NetworkConnectSuccess": "network|connect|success",
     "SdpLoginFailed": "sdp|login_failed" ,
     "NetworkDisconnectAppInitiated": "network|disconnect_app_initiated",
     "SdpLoginSuccess": "sdp|login_success" ,
     "SdpSocketDropped":"sdp|socket|dropped",
     "SettingsGeneralDisableAutomaticUpdates": "settings|general|disable_automatic_updates",
     "SettingsGeneralDisableSendCrashReport": "settings|general|disable_send_crash_report",
     "SettingsGeneralEnableStartMinimized": "settings|general|enable_start_minimized",
     "SettingsNetworkDisableAlwaysOn": "settings|network|disable_always_on",
     "SettingsNetworkDisableWifiSecurity": "settings|network|disable_wifi_security",
     "SettingsNetworkEnableAlwaysOn": "settings|network|enable_always_on",
     "SettingsNetworkTrustedNetworkAdded": "settings|network|trusted_network_added",
     "SupportUserGuidesEnter": "support|user_guides|enter",
     "SystemAwake": "system|awake",
     "SystemSleep": "system|sleep",
     "TrafficSwgUrlBlock": "traffic|swg|url_block",
     "TrafficSwgUrlWarn": "traffic|swg|url_warn",
     "WorkspaceLaunch": "workspace|launch",
     "SdpLogin" :"sdp|login"
     }

n = len(sys.argv)
argv = sys.argv[1:3]

for idx, arg in enumerate(sys.argv):
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

s3_event_name =  arg
str_date_filter = str(dt)
l = len(str(int_hr))
if len(str(int_hr)) == 2 :
    hr=str(int_hr)
else:
    hr = '0'+str(int_hr)

file_path = "data/padme/agents/event=" + s3_event_name + "/dt=" + str_date_filter + "-" +hr + "/"
bucket_name = "p81-yakron-msk"

async def redshift_conn(name):
      conn = rsc.connect(
        host ='snowplow-com-safervpn-main-red-resredshiftcluster-j4nqz5chy6qd.czqepfubfdjr.us-east-2.redshift.amazonaws.com',
        port =5439,
        database ='snowplow',
        user ='data_viewer',
        password ='DEF1987leppard?!'
      )
      try:
        rd_event_name = dc_events[s3_event_name]
      except ValueError :
        print('Please chooce right event')

      filter_from_time = datetime.datetime.combine(dt, datetime.time(int_hr))
      filter_to_time = datetime.datetime.combine(dt, datetime.time(int_hr + 1))

      cursor  = conn.cursor()
      cursor_compare = conn.cursor()

      cur = cursor.execute("select count(*) from lake.agent_events t where t.event_name =%s and t.server_tstamp >= %s and t.server_tstamp <= %s ;",(rd_event_name,filter_from_time,filter_to_time))
      cursor_compare = cursor_compare.execute("select top 1 * from lake.agent_events t where t.event_name =%s and t.server_tstamp >= %s and t.server_tstamp <= %s ;",(rd_event_name,filter_from_time,filter_to_time))

      row = cur.fetchone()
      while row:
        rs_count = (row)
        row = cursor.fetchone()

      cursor.close()
      cursor_compare.close()
      conn.close()

      text =  'SnowPlow EVENT ROWS COUNT - ' + str(rs_count).title()
      print(text)


async def s3(name):
      s3_client = boto3.client("s3")
      all_count = 0
    #----------------------------
      response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=file_path)
      files = response.get("Contents")
      if isinstance(files, list) and len(files)> 0:
          for file in files:
              resp = s3_client.select_object_content(
                Bucket=bucket_name,
                Key=file['Key'], #'data/padme/agents/event=SdpLogin/dt=2022-03-29-05/yarkon_padme_SdpLogin+0+0000708834.json',
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

          text = "Count Comparing - Event =" + ' '  + s3_event_name.upper() + ' \n' + 'DATE  - ' +  str_date_filter.upper().title() + '  hour - ' +  hr + '  \n' \
                 'YARKON S3 BUCKET ROWS COUNT = '+ str(all_count).title() + ' -  IN  - ' + str(len(files)) + ' FILES'
          print(text)

      else:
        print("No file Found")

      """ count row by read lines
      response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="data/padme/agents/event=SdpLogin/dt=2022-03-29-05/")
      files = response.get("Contents")
      nlines = 0
      for file in files:
        listfile = (file['Key'].split('/'))
        file_name = listfile[len(listfile)-1]
        print(file_name)
        file_n = file_path + file_name
        obj = s3_client.get_object(Bucket='p81-yakron-msk', Key=file_n)
        for _ in obj['Body'].iter_lines():
          nlines += 1
        print(nlines)
    """

async def main():
      print('START' + '------' + str(datetime.datetime.now()))
      await s3('s3')
      await redshift_conn('redshift')
      print('END' + '------' + str(datetime.datetime.now()))

if __name__ == "__main__":
    asyncio.run(main())


