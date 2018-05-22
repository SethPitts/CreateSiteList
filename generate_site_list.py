import pandas as pd
from collections import defaultdict, namedtuple, OrderedDict, Callable
import csv


class DefaultOrderedDict(OrderedDict):
    # Source: http://stackoverflow.com/a/6190500/562769
    def __init__(self, default_factory=None, *a, **kw):
        if (default_factory is not None and
           not isinstance(default_factory, Callable)):
            raise TypeError('first argument must be callable')
        OrderedDict.__init__(self, *a, **kw)
        self.default_factory = default_factory

    def __getitem__(self, key):
        try:
            return OrderedDict.__getitem__(self, key)
        except KeyError:
            return self.__missing__(key)

    def __missing__(self, key):
        if self.default_factory is None:
            raise KeyError(key)
        self[key] = value = self.default_factory()
        return value

    def __reduce__(self):
        if self.default_factory is None:
            args = tuple()
        else:
            args = self.default_factory,
        return type(self), args, None, None, self.items()

    def copy(self):
        return self.__copy__()

    def __copy__(self):
        return type(self)(self.default_factory, self)

    def __deepcopy__(self, memo):
        import copy
        return type(self)(self.default_factory,
                          copy.deepcopy(self.items()))

    def __repr__(self):
        return 'OrderedDefaultOrderedDict(%s, %s)' % (self.default_factory,
                                               OrderedDict.__repr__(self))
# get NDB and NDC data from sas

ndb_site_data = pd.read_sas(r'G:\NIDADSC\spitts\SAS_Projects\NDB\siteinfo.sas7bdat', format='sas7bdat')
ndc_site_data = pd.read_sas(r'G:\NIDADSC\spitts\SAS_Projects\NDC\siteinfo.sas7bdat', format='sas7bdat')

platforms = [ndb_site_data, ndc_site_data]

# Containers for data you need
protocol_site_info = DefaultOrderedDict(dict)
protocol_node_info = DefaultOrderedDict(dict)
all_site_info = DefaultOrderedDict(dict)
all_node_info = DefaultOrderedDict(dict)

site_info_tuple = namedtuple('Site_Info', ['prot', 'site', 'status', 'node'])
collection_of_sites = []
for platform in platforms:
    for index, row in platform.iterrows():
        prot = row.PROT.decode('utf-8')
        if prot == '00 .':
            continue
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

        collection_of_sites.append(site_info_tuple(prot, site, closed, node))

protocol_info = DefaultOrderedDict(dict)
protocol_and_site_info = []
all_prots = sorted(protocol_node_info.keys())
protocol_and_site_info.append(['Site/Node'] + ['CTN-{}'.format(protocol) for protocol in all_prots])
for prot in all_prots:
    protocol_info[prot]['CTN-NODES'] = len(protocol_node_info[prot].keys())

protocol_and_site_info.append(['CTN-NODES'] + [protocol_info[prot]['CTN-NODES']
                              for prot in protocol_info.keys()])

for prot in all_prots:
    protocol_info[prot]['CTN-SITES'] = sum(protocol_site_info[prot].values())

protocol_and_site_info.append(['CTN-SITES'] + [protocol_info[prot]['CTN-SITES']
                              for prot in protocol_info.keys()])

all_nodes = set([site.node for site in collection_of_sites])
final_node_info = DefaultOrderedDict(dict)
for node in all_nodes:
    for protocol in all_prots:
        final_node_info[node][protocol] = [site for site in collection_of_sites
                                           if site.prot == protocol
                                           if site.node == node
                                           ]

for node, node_info in final_node_info.items():
    for prot in all_prots:
        if node_info.get(prot) is None:
            node_info[prot] = []

for node, node_info in final_node_info.items():
    protocol_and_site_info.append([node] + [len(item) for item in node_info.values()])
    site_specific_info = DefaultOrderedDict(dict)
    for protocol in all_prots:
        if node_info.get(protocol) != []:
            sites_in_protocol = node_info.get(protocol)
        else:
            sites_in_protocol = []
        for site in sites_in_protocol:
            site_name = site.site
            site_status = site.status
            site_specific_info[site_name][protocol] = site_status
    for site in site_specific_info:
        for prot in all_prots:
            if site_specific_info[site].get(prot) is None:
                site_specific_info[site][prot] = ""
    for site, site_info in site_specific_info.items():
        protocol_and_site_info.append([site] + list([site_info[prot] for prot in all_prots]))

with open('all_site_info.csv', 'w', newline="") as outfile:
    csvwriter = csv.writer(outfile)
    csvwriter.writerows(protocol_and_site_info)

# protocol_info_df = pd.DataFrame.from_dict(protocol_info)
# all_node_info_df = pd.DataFrame.from_dict(all_node_info)
# all_site_info_df = pd.DataFrame.from_dict(all_site_info)
#
# protocol_excel_writer = pd.ExcelWriter('protocol_info.xlsx')
#
#
#
# protocol_info_df.to_excel(protocol_excel_writer, sheet_name='protocol_info')
# all_node_info_df.to_excel(protocol_excel_writer, sheet_name='node_info')
# all_site_info_df.to_excel(protocol_excel_writer, sheet_name='site_info')
#
# protocol_excel_writer.save()