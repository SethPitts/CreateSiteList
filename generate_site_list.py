import pandas as pd
from collections import defaultdict, namedtuple

# get NDB and NDC data from sas

ndb_site_data = pd.read_sas('siteprot.sas7bdat', format='sas7bdat')
ndc_site_data = pd.read_sas('siteprot.sas7bdat', format='sas7bdat')

platforms = [ndb_site_data, ndc_site_data]

# Containers for data you need
protocol_site_info = defaultdict(dict)
protocol_node_info = defaultdict(dict)
all_site_info = defaultdict(dict)
all_node_info = defaultdict(dict)

site_info_tuple = namedtuple('Site_Info', ['prot', 'site', 'status', 'node'])
collection_of_sites = []
for platform in platforms:
    print(platform.keys())
    for index, row in platform.iterrows():
        prot = row.PROT.decode('utf-8')
        site = row.SITECODE.decode('utf-8')
        closed = row.CLOSED
        if closed == 0.0:
            closed = "A"
        else:
            closed = "C"
        node = row.SITECODE
        if type(node) == bytes:
            node = node.decode('utf-8')
        else:
            continue
        current_info = site_info_tuple(prot, site, closed, node)
        if protocol_site_info[prot].get(site) is not None:
            protocol_site_info[prot][site] += 1
        else:
            protocol_site_info[prot][site] = 1

        if protocol_node_info[prot].get(node) is not None:
            protocol_node_info[prot][node] += 1
        else:
            protocol_node_info[prot][node] = 1

        collection_of_sites.append(site_info_tuple(prot, site, closed, node))

protocol_info = defaultdict(dict)
protocol_and_site_info = []
all_prots = protocol_node_info.keys()
print(all_prots)
print(collection_of_sites)
for prot, node in protocol_node_info.items():
    protocol_info[prot]['CTN-NODES'] = len(node.keys())

protocol_and_site_info.append(['CTN-NODES'] + [protocol_info[prot]['CTN-NODES']
                              for prot in protocol_info.keys()])

print(protocol_and_site_info)
print('-------------------------------')

for prot, site in protocol_site_info.items():
    protocol_info[prot]['CTN-SITES'] = sum(site.values())

protocol_and_site_info.append(['CTN-SITES'] + [protocol_info[prot]['CTN-SITES']
                              for prot in protocol_info.keys()])
print(protocol_and_site_info)

final_node_info = defaultdict(dict)
for site in collection_of_sites:
    prot = site.prot
    site_name = site.site
    closed = site.status
    node = site.node
    final_node_info[node][prot] = defaultdict(dict)
    final_node_info[node][prot][site_name] = closed

print(final_node_info)
print("_____________________________")
# for key, value in all_site_info.items():
#     print(key, value, 'site')

node = [['NODE'] + ['NODES IN PROT']]

protocol_info_df = pd.DataFrame.from_dict(protocol_info)
all_node_info_df = pd.DataFrame.from_dict(all_node_info)
all_site_info_df = pd.DataFrame.from_dict(all_site_info)

protocol_excel_writer = pd.ExcelWriter('protocol_info.xlsx')



protocol_info_df.to_excel(protocol_excel_writer, sheet_name='protocol_info')
all_node_info_df.to_excel(protocol_excel_writer, sheet_name='node_info')
all_site_info_df.to_excel(protocol_excel_writer, sheet_name='site_info')

protocol_excel_writer.save()