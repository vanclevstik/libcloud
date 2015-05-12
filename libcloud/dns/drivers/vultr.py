import pdb

from libcloud.utils.py3 import urllib
from libcloud.common.vultr import VultrConnection, VultrResponse
from libcloud.dns.base import DNSDriver, Zone, Record
from libcloud.dns.types import ZoneDoesNotExistError, RecordDoesNotExistError
from libcloud.dns.types import ZoneAlreadyExistsError,RecordAlreadyExistsError
from libcloud.dns.types import Provider, RecordType



class ZoneRequiredException(Exception):
    pass


class VultrDNSResponse(VultrResponse):
    pass


class VultrDNSConnection(VultrConnection):
    responseCls = VultrDNSResponse


class VultrDNSDriver(DNSDriver):
    type = Provider.VULTR
    name = 'Vultr DNS'
    website = 'http://www.vultr.com/'
    connectionCls = VultrDNSConnection

    RECORD_TYPE_MAP = {

            RecordType.A:'A',
            RecordType.AAAA:'AAAA',
            RecordType.TXT:'TXT',
            RecordType.CNAME:'CNAME',
            RecordType.MX:'MX',
            RecordType.NS:'NS',
            RecordType.SRV:'SRV',
            }

    def _to_zone(self, item):
        """
        Build an object `Zone` from the item dictionary
        :param item: item to build the zone from
        :type item: `dictionary`

        :rtype: :instance: `Zone`
        """
        type = 'master'
        extra = {'date_created':item['date_created']}

        zone = Zone(id=item['domain'],domain=item['domain'],driver=self,
                type=type, ttl=None,extra=extra)

        return zone

    def _to_zones(self, items):
        """
        Returns a list of `Zone` objects.

        :param: items: a list that contains dictionary objects to be passed
        to the _to_zone function.
        :type items: ``list``
        """
        zones = []
        for item in items:
            zones.append(self._to_zone(item))

        return zones

    def _to_record(self, item, zone):

        extra = {}

        if item.get('priority'):
            extra['priority'] = item['priority']

        type = self._string_to_record_type(item['type'])
        record = Record(id=item['RECORDID'],name=item['name'],type=type,
                data=item['data'],zone=zone,driver=self, extra=extra)

        return record

    def _to_records(self, items, zone):
        records = []
        for item in items:
            records.append(self._to_record(item, zone=zone))

        return records


    def list_zones(self):
        """
       Function that lists zones.
        """

        action = '/v1/dns/list'
        params = {'api_key':self.key}
        zones = self.connection.request(action=action, params=params).parse_body()[0]

        zones = self._to_zones(zones)

        return zones

    def get_zone(self, zone_id):
        """
        Returns a `Zone` instance.

        :param zone_id: name of the zone user wants to get.
        :type zone_id: ``str``
        :rtype: :class:`Zone`
        """
        ret_zone = None

        action = '/v1/dns/list'
        params = {'api_key':self.key}
        data = self.connection.request(action=action, params=params).parse_body()[0]
        zones = self._to_zones(data)

        if not self.zone_exists(zone_id, zones):
            raise ZoneDoesNotExistError(value=None,zone_id=zone_id,driver=self)

        for zone in zones:
            if zone_id == zone.domain:
                ret_zone = zone

        return ret_zone

    def get_record(self, zone_id, record_id):
        """
        Returns a Record instance.

        :param zone_id: name of the required zone
        :type zone_id: ``str``

        :param record_id: ID of the required record
        :type record_id: ``str``
        :rtype: :class: `Record`
        """
        ret_record = None
        zone = self.get_zone(zone_id=zone_id)
        records = self.list_records(zone=zone)

        if not self.record_exists(record_id,records):
            raise RecordDoesNotExistError(value='', driver=self, record_id=record_id)

        for record in records:
            if record_id == record.id:
                ret_record = record

        return ret_record

    def list_records(self, zone):
        """
        Returns a list of records for the provided zone.

        :param zone: zone to list records for
        :type zone: `Zone`
        :rtype: list of :class: `Record`
        """
        if not isinstance(zone, Zone):
            raise ZoneRequiredException('zone should be of type Zone')

        zones = self.list_zones()

        if not self.zone_exists(zone.domain, zones):
            raise ZoneDoesNotExistError(value='', driver=self, zone_id=
                    zone.domain)

        records = []
        action = '/v1/dns/records'
        params = {'domain':zone.domain}
        pure_data = self.connection.request(action=action, params=params)
        response_records = pure_data.parse_body()[0]


        records.append(self._to_records(response_records, zone=zone))

        return records[0]


    def create_zone(self, zone_id, type='master', ttl=None, extra=None):
        """
        Returns a `Zone` object.

        :param domain: Zone domain name, (e.g. example.com).
        :type domain: ``str``

        """
        if extra and extra.get('serverip'):
            serverip = extra['serverip']


        params = {'api_key':self.key}
        data = urllib.urlencode({'domain':zone_id,'serverip':serverip})
        action = '/v1/dns/create_domain'
        zones = self.list_zones()
        if self.zone_exists(zone_id, zones):
            raise ZoneAlreadyExistsError(value='', driver=self, zone_id=zone_id)

        self.connection.request(params=params,action=action,data=data,
                method='POST',headers=self.HEADERS)
        zone = Zone(id=zone_id,domain=zone_id,type=type,ttl=ttl,
                driver=self,extra=extra)

        return zone

    def delete_zone(self, zone):

        action = '/v1/dns/delete_domain'
        params = {'api_key':self.key}
        data = urllib.urlencode({'domain':zone.domain})
        zones = self.list_zones()
        if not self.zone_exists(zone.domain, zones):
            raise ZoneDoesNotExistError(value='', driver=self, zone_id=zone.domain)

        response = self.connection.request(params=params,action=action,
                data=data,method='POST',headers=self.HEADERS)

        return response.status == 200

    def delete_record(self, record):

        action = '/v1/dns/delete_record'
        params = {'api_key':self.key}
        data = urllib.urlencode({'RECORDID':record.id, 'domain':record.zone.domain})

        zone_records = self.list_records(record.zone)
        if not self.record_exists(record.id, zone_records):
            raise RecordDoesNotExistError(value='', driver=self, record_id=record.id)

        response = self.connection.request(action=action, params=params,
                data=data, method='POST', headers=self.HEADERS)

        return response.status == 200

    def create_record(self, name, zone, type, data, extra=None):

        old_records_list = self.list_records(zone=zone)
        #check if record already exists
        #if exists raise RecordAlreadyExistsError
        for record in old_records_list:
            if record.name == name:
                raise RecordAlreadyExistsError(value='', driver=self,
                        record_id=record.id)
        record = None

        if extra and extra.get('priority'):
            priority = int(extra['priority'])

        MX = self.RECORD_TYPE_MAP.get('MX')
        SRV = self.RECORD_TYPE_MAP.get('SRV')

        post_data = {'domain':zone.domain, 'name':name,
                'type':self.RECORD_TYPE_MAP.get('type'), 'data':data}


        if type == MX or type == SRV:
            post_data['priority'] = priority


        post_data = {'domain':zone.domain, 'name':name,
                'type':self.RECORD_TYPE_MAP.get(type), 'data':data}


        encoded_data = urllib.urlencode(post_data)
        params = {'api_key':self.key}
        action = '/v1/dns/create_record'

        self.connection.request(action=action, params=params, data=encoded_data,
                method='POST', headers=self.HEADERS)

        updated_zone_records = zone.list_records()
        for record in updated_zone_records:
            if record.name == name:
                record = record

        return record

    def zone_exists(self, zone_id, zones_list):
        """
        Function to check if a `Zone` object exists.
        :param zone_id: Name of the `Zone` object.
        :type zone_id: ``str``
        :param zones_list: A list containing `Zone` objects
        :type zones_list: ``list``

        :rtype: Returns `True` or `False`
        """

        zone_ids = []
        for zone in zones_list:
            zone_ids.append(zone.domain)

        return zone_id in zone_ids

    def record_exists(self, record_id, records_list):
        """
        """
        record_ids = []
        for record in records_list:
            record_ids.append(record.id)

        return record_id in record_ids

