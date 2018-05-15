import pandas as pd
from collections import defaultdict, namedtuple

# get NDB and NDC data from sas

ndb_site_data = pd.read_sas(r'G:\NIDADSC\spitts\SAS_Projects\NDB\siteinfo.sas7bdat', format='sas7bdat')
ndc_site_data = pd.read_sas(r'G:\NIDADSC\spitts\SAS_Projects\NDC\siteinfo.sas7bdat', format='sas7bdat')

platforms = [ndb_site_data, ndc_site_data]

# Containers for data you need
protocol_site_info = defaultdict(dict)
protocol_node_info = defaultdict(dict)
all_site_info = defaultdict(dict)
all_node_info = defaultdict(dict)

site_info_tuple = namedtuple('Site_Info', ['PROT', 'SITE', 'CLOSED', 'NODE'])

for platform in platforms:
    for index, row in platform.iterrows():
        prot = row.PROT.decode('utf-8')
        site = row.SITENAME.decode('utf-8')
        closed = row.CLOSED
        if closed == 0.0:
            closed = "A"
        else:
            closed = "C"
        node = row.Node_name
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

        if all_node_info[prot].get(node) is not None:
            all_node_info[prot][node] += 1
        else:
            all_node_info[prot][node] = 1

        all_site_info[prot][site] = (closed, node)

protocol_info = defaultdict(dict)
protocol_and_site_info = []
for prot, node in protocol_node_info.items():
    print(prot, node)
    protocol_info[prot]['CTN-NODES'] = len(node.keys())

print('-------------------------------')
for prot, site in protocol_site_info.items():
    print(prot, site)
    protocol_info[prot]['CTN-SITES'] = sum(site.values())

for prot, info in protocol_site_info.items():
    protocol_and_site_info.append(['CTN-NODES'] + protocol_info[prot]['CTN-NODES'].values())
    protocol_and_site_info.append(['CTN-SITES'] + protocol_info[prot]['CTN-SITES'].values())

just_node_info = defaultdict(dict)
for prot, node_info in all_node_info:
    node_name, values = node_info.items()
    just_node_info[node_name][prot] = sum(values)
just_site_info = defaultdict(dict)
for prot, site_info in all_site_info:
    site_name, values = site_info.items()
    just_site_info[site_name][prot] =


for node, node_info in just_node_info.items():
    protocol_and_site_info.append([node] + node_info.values())
    for


protocol_info_df = pd.DataFrame.from_dict(protocol_info)
all_node_info_df = pd.DataFrame.from_dict(all_node_info)
all_site_info_df = pd.DataFrame.from_dict(all_site_info)

protocol_excel_writer = pd.ExcelWriter('protocol_info.xlsx')



protocol_info_df.to_excel(protocol_excel_writer, sheet_name='protocol_info')
all_node_info_df.to_excel(protocol_excel_writer, sheet_name='node_info')
all_site_info_df.to_excel(protocol_excel_writer, sheet_name='site_info')

protocol_excel_writer.save()