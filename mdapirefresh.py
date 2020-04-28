import requests, json
#these are all the variables you'll need to set
ts_username = '<username>'
ts_password = '<password>'
ts_url = '<server_url>'
site = '<sitename>'
schedule_name = '<Daily>'
table_name = '<table_name>'

#these are all default variables
needs_refresh = []
on_schedule = []
run_now = []
headers = {'accept': 'application/json','content-type': 'application/json'}
payload = { "credentials": {"name": ts_username, "password": ts_password, "site" :{"contentUrl": site} } }
req = requests.post(ts_url + 'api/3.5/auth/signin', json=payload, headers=headers, verify=True)
response =json.loads(req.content)
token = response["credentials"]["token"]
site_id = response["credentials"]["site"]["id"]
auth_headers = {'accept': 'application/json','content-type': 'application/json','x-tableau-auth': token}
#define our metadata api query to return datasources with our chosen table
mdapi_query = '''query relatedDatasources {
databaseTables (filter: {name: "'''+ table_name + '''"}){
    downstreamDatasources {
      luid
    }
}
}'''

#return all schedules
schedule_list = requests.get(ts_url + '/api/3.5/schedules/', headers = auth_headers, verify=True)
schedule_list = json.loads(schedule_list.text)['schedules']['schedule']

#identify your chosen schedule
for i in schedule_list:
    if i['name'] == schedule_name:
            schedule_id = i['id']
            print(i['name'])

#identify all associated tasks from that schedule
task_list = requests.get(ts_url + '/api/3.5/sites/' + site_id + '/schedules/' + schedule_id + '/extracts', headers = auth_headers, verify=True)
task_list = json.loads(task_list.text)['extracts']['extract']
for i in task_list:
    on_schedule.append(i['id'])

#get datasources with table
metadata_query = requests.post(ts_url + '/api/metadata/graphql', headers = auth_headers, verify=True, json = {"query": mdapi_query})
mdapi_result = json.loads(metadata_query.text)
#find the LUID for each of those datasources
for i in mdapi_result['data']['databaseTables'][0]['downstreamDatasources']:
    needs_refresh.append(i['luid'])
#find the intersections of datasources with the table and datasources on our schedule
run_now = list(set(needs_refresh).intersection(on_schedule))

#run tasks
for i in run_now:
    requests.post(ts_url + '/api/3.5/sites/'+ site_id + '/tasks/extractRefreshes/' + i + '/runNow')
