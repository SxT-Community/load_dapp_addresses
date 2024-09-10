from pathlib import Path
from spaceandtime import SpaceAndTime, SXTTable


# setup environment and default user
sxt = SpaceAndTime(Path('./private.env').resolve(), 
                   application_name="load_dapp_address")
sxt.authenticate()

# setup table definition
tbl = SXTTable(from_file=Path('./private.env').resolve(),
               access_type=sxt.TABLE_ACCESS.PUBLIC_READ,
               SpaceAndTime_parent= sxt )
if not tbl.exists: tbl.create()

tbl.delete(where='')

# get all existing records, so we don't need to update existing every time 
# existing_data = tbl.select(f'select * from {tbl.table_name} order by DAPP_Name, DAPP_Addr', row_limit=10000)



# process new data to a list of dictionaries
new_data_path = Path('./src/DefiLlama-Adapters/projects').resolve()
new_data = []

for file in new_data_path.glob('**/index.js'):
    
    # open each file
    with open(file.resolve(), 'r') as fh:
        content = fh.read()

    # see if we can find an EVM address
    addr_split = content.split('"0x')
    addr_seen = [] 
    if len(addr_split) == 0: continue

    for addr_line in addr_split[1:]:
        dapp_addr = f'0x{addr_line[:40]}'.lower()
        if dapp_addr in addr_seen: continue
        dapp_name = file.parent.name
        dapp_type = ''
        new_data.append({"DAPP_Name":dapp_name, "DAPP_Addr":dapp_addr, "DAPP_Type":dapp_type})
        addr_seen.append(dapp_addr)
        

#TODO: select from existing and only process deltas

# perform insert
start = 0
for end in range(1000, len(new_data), 1000):
    tbl.insert.with_list_of_dicts(new_data[start:end])
    sxt.authenticate()
    start = end+1

tbl.logger.info("Complete!")