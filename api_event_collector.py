import requests, json, time, datetime, os, sys

DIR_NAME = f'data-{datetime.datetime.now()}'
BASE_URL = 'https://api.open511.gov.bc.ca'

def fetch_archived_events(offset=0, part_num=0):
    events = []
    limit = 500
    event_threshold = 5000
    sleep_dur = 10
    offset_to_reset_limit = None
    
    while True:
        url = f"{BASE_URL}/events?limit={limit}&offset={offset}&status=ARCHIVED"
        response = requests.get(url, timeout=60)

        if offset_to_reset_limit and offset > offset_to_reset_limit:
            limit = 500
            offset_to_reset_limit = None

        if response.status_code != 200:
            # The Drive BC API limits the request rate. THe following is a back-off response in the case of throttling.
            if response.status_code == 429:
                print(f'Error {response.status_code} returned, sleeping for {sleep_dur} sec.')
                time.sleep(sleep_dur)
                continue
            # Sometimes there are problematic records that cause a server error if requested. The following finds and skips said record.
            if response.status_code == 500:
                if limit == 1:
                    print(f'Offset {offset} skipped, resetting limit.')
                    offset += 1
                    limit = 500
                else:
                    print(f'Error {response.status_code} returned, reducing limit from {limit} to {limit // 2}')
                    offset_to_reset_limit = offset + limit
                    limit //= 2
                continue
            print(f"Error code {response.status_code}, terminating invoker.")
            break
        
        try:
            data = response.json()
        except json.JSONDecodeError as e:
            print('JSON decoding error, re-attempting fetch.')

        new_events = data.get("events", [])
        events.extend(new_events)
        offset += len(new_events)
        print(f'Archived Events Collected: {len(events)}, Current Offset: {offset}')

        if len(events) >= event_threshold:
            save_as_json(events, 'ARCHIVED', part_num)
            events = []
            part_num += 1
        
        if not new_events:
            print('No new events received, ending retrieval.')
            break

    save_as_json(events, 'ARCHIVED', part_num)
    data = {"offset": offset}
    filename = 'termination.json'
    print(f'Saving last offset in {filename}')
    with open(f'{DIR_NAME}/{filename}', 'w') as json_file:
        json.dump(data, json_file, indent=4)

def fetch_active_events():
    events = []
    offset = 0
    limit = 500
    part_num = 0
    sleep_dur = 10
    
    while True:
        url = f"{BASE_URL}/events?limit={limit}&offset={offset}"
        response = requests.get(url, timeout=60)

        if response.status_code != 200:
            # The Drive BC API limits the request rate. THe following is a back-off response in the case of throttling.
            if response.status_code == 429:
                print(f'Error {response.status_code} returned, sleeping for {sleep_dur} sec.')
                time.sleep(sleep_dur)
                continue
            print(f"Error code {response.status_code}, terminating invoker at offset {offset} and limit {limit}.")
            break
        
        data = response.json()
        new_events = data.get("events", [])
        events.extend(new_events)
        offset += len(new_events)
        
        if not new_events:
            print(f'{len(events)} active events retrieved.')
            save_as_json(events, 'ACTIVE', part_num)
            break

def fetch_areas():
    response = requests.get(f"{BASE_URL}/areas", timeout=30)
    data = response.json()
    areas = data.get("areas")
    print(f'{len(areas)} areas retrieved.')
    save_as_json(areas, 'AREA', 0)

def save_as_json(data, subdir, partiton_number):
    print(f'Saving {subdir}-{partiton_number}')
    with open(f'{DIR_NAME}/{subdir}/records-{partiton_number}.json', "w") as f:
        json.dump(data, f, indent=4)

def init_data_dir():
    os.mkdir(DIR_NAME)
    os.mkdir(f'{DIR_NAME}/ACTIVE')
    os.mkdir(f'{DIR_NAME}/ARCHIVED')
    os.mkdir(f'{DIR_NAME}/AREA')
    print(f"Directory '{DIR_NAME}' created successfully.")

if __name__ == '__main__':

    last_offset = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    part_num = int(sys.argv[2]) if len(sys.argv) > 2 else 0

    start_time = time.time()
    init_data_dir()

    fetch_areas()
    fetch_active_events()
    fetch_archived_events(last_offset, part_num)

    print(f'Process finished in {(time.time() - start_time) / 60} min.')