# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

__all__ = [
    'HPCloudDNSDriver',
]


from libcloud.common.openstack import OpenStackDriverMixin
from libcloud.compute.drivers.openstack import OpenStack_1_1_Response
from libcloud.compute.drivers.openstack import OpenStackBaseConnection
from libcloud.compute.types import LibcloudError
from libcloud.dns.base import DNSDriver, Zone, Record
from libcloud.dns.types import Provider, RecordType
from libcloud.dns.types import ZoneDoesNotExistError
from libcloud.dns.types import RecordDoesNotExistError
from libcloud.dns.types import ZoneAlreadyExistsError
from libcloud.dns.types import RecordAlreadyExistsError

import httplib
import json

VALID_ZONE_EXTRA_PARAMS = ['email', 'comment', 'ns1']
VALID_RECORD_EXTRA_PARAMS = ['ttl', 'comment', 'priority']


class HPCloudDNSResponse(OpenStack_1_1_Response):
    """
    HPCloud DNS Response class.
    """
    def parse_error(self):
        status = int(self.status)
        context = self.connection.context
        body = self.parse_body()

        if status == httplib.NOT_FOUND:
            if context['resource'] == 'zone':
                raise ZoneDoesNotExistError(value='', driver=self,
                                            zone_id=context['id'])
            elif context['resource'] == 'record':
                raise RecordDoesNotExistError(value='', driver=self,
                                              record_id=context['id'])
        if status == httplib.CONFLICT:
            if context['resource'] == 'zone':
                raise ZoneAlreadyExistsError(value=context['value'],
                                             driver=self,
                                             zone_id=None)
            elif context['resource'] == 'record':
                raise RecordAlreadyExistsError(value=context['value'],
                                               driver=self,
                                               record_id=None)
        if body:
            if 'code' and 'message' in body:
                err = '%s - %s (%s)' % (body['code'], body['message'],
                                        body['errors'][0]['message'])
                return err

        raise LibcloudError('Unexpected status code: %s' % (status))


class HPCloudDNSConnection(OpenStackBaseConnection):

    _auth_version = '3.x_password'
    auth_url = 'https://region-a.geo-1.dns.hpcloudsvc.com/'
    accept_format = 'application/json'
    default_content_type = 'application/json'
    responseCls = HPCloudDNSResponse

    def get_endpoint(self):
        return 'https://region-a.geo-1.dns.hpcloudsvc.com/v1'


class HPCloudDNSDriver(DNSDriver, OpenStackDriverMixin):
    name = 'HP Public Cloud (Helion)'
    website = 'http://www.hpcloud.com/'
    connectionCls = HPCloudDNSConnection
    type = Provider.HPCLOUD

    RECORD_TYPE_MAP = {
        RecordType.A: 'A',
        RecordType.AAAA: 'AAAA',
        RecordType.CNAME: 'CNAME',
        RecordType.MX: 'MX',
        RecordType.NS: 'NS',
        RecordType.PTR: 'PTR',
        RecordType.SRV: 'SRV',
        RecordType.TXT: 'TXT',
    }

    def __init__(self, key, secret, ex_domain_name, ex_tenant_name,
                 secure=True, host=None, port=None, **kwargs):
        """
        Note: ex_tenant_name  and ex_domain_name argument is required for HP cloud.
        """
        OpenStackDriverMixin.__init__(self,
                                      ex_domain_name=ex_domain_name,
                                      ex_tenant_name=ex_tenant_name,
                                    )
        super(HPCloudDNSDriver, self).__init__(key=key, secret=secret,
                                               secure=secure, host=host,
                                               port=port,
                                               **kwargs)

    def get_zone(self, zone_id):
        self.connection.set_context({'resource': 'zone', 'id': zone_id})
        response = self.connection.request(
            action='/domains/{}'.format(zone_id))
        zone = self._to_zone(data=response.object)
        return zone

    def iterate_zones(self):
        response = self.connection.request(action='/domains')
        zones_list = response.object['domains']
        for item in zones_list:
            yield self._to_zone(item)

    def get_record(self, zone_id, record_id):
        zone = self.get_zone(zone_id=zone_id)
        self.connection.set_context({'resource': 'record', 'id': record_id})
        response = self.connection.request(
            action='/domains/{}/records/{}'.format(zone_id, record_id))
        record = self._to_record(data=response.object, zone=zone)
        return record

    def iterate_records(self, zone):
        self.connection.set_context({'resource': 'zone', 'id': zone.id})
        response = self.connection.request(
            action='/domains/{}/records'.format(zone.id))
        records_list = response.object['records']
        for item in records_list:
            yield self._to_record(item, zone)

    def create_zone(self, domain, type='master', ttl=7200, extra=None):
        extra = extra if extra else {}

        # Email address is required
        if 'email' not in extra:
            raise ValueError('"email" key must be present in extra dictionary')

        payload = {'name': domain, 'email': extra['email']}

        if ttl:
            payload['ttl'] = ttl

        if 'comment' in extra:
            payload['comment'] = extra['comment']
        data = json.dumps(payload)
        self.connection.set_context({'resource': 'zone', 'value': domain})
        response = self.connection.request(action='/domains',
                                           method='POST', data=data)
        zone = self._to_zone(response.object)
        return zone

    def create_record(self, name, zone, type, data, extra=None):
        # Name must be a FQDN - e.g. if domain is "foo.com" then a record
        # name is "bar.foo.com"
        extra = extra if extra else {}

        name = self._to_full_record_name(domain=zone.domain, name=name)
        data = {'name': name, 'type': self.RECORD_TYPE_MAP[type],
                'data': data}

        if 'ttl' in extra:
            data['ttl'] = int(extra['ttl'])
        else:
            data['ttl'] = 3600

        if 'description' in extra:
            data['description'] = extra['data']
        else:
            data['description'] = name

        if 'priority' in extra:
            data['priority'] = int(extra['priority'])

        data = json.dumps(data)
        self.connection.set_context({'resource': 'record', 'value': name})
        response = self.connection.request(
            action='/domains/{}/records'.format(zone.id),
            data=data,
            method='POST',
        )
        record = self._to_record(data=response.object, zone=zone)
        return record

    def delete_zone(self, zone):
        self.connection.set_context({'resource': 'zone', 'id': zone.id})
        self.connection.request(
            action='/domains/{}'.format(zone.id),
            method='DELETE')
        return True

    def delete_record(self, record):
        self.connection.set_context({'resource': 'record', 'id': record.id})
        self.connection.request(
            action='/domains/{}/records/{}'.format(record.zone.id, record.id),
            method='DELETE')
        return True

    def _to_zone(self, data):
        id = data['id']
        domain = data['name']
        type = 'master'
        ttl = data.get('ttl', 0)
        extra = {}

        if 'email' in data:
            extra['email'] = data['email']

        if 'description' in data:
            extra['description'] = data['description']

        zone = Zone(id=str(id), domain=domain, type=type, ttl=int(ttl),
                    driver=self, extra=extra)
        return zone

    def _to_record(self, data, zone):
        id = data['id']
        fqdn = data['name']
        name = self._to_partial_record_name(domain=zone.domain, name=fqdn)
        type = self._string_to_record_type(data['type'])
        record_data = data['data']
        extra = {'fqdn': fqdn}

        for key in VALID_RECORD_EXTRA_PARAMS:
            if key in data:
                extra[key] = data[key]

        record = Record(id=str(id), name=name, type=type, data=record_data,
                        zone=zone, driver=self, extra=extra)
        return record

    def _to_full_record_name(self, domain, name):
        """
        Build a FQDN from a domain and record name.

        :param domain: Domain name.
        :type domain: ``str``

        :param name: Record name.
        :type name: ``str``
        """
        if name:
            name = '%s.%s' % (name, domain)
        else:
            name = domain

        return name

    def _to_partial_record_name(self, domain, name):
        """
        Remove domain portion from the record name.

        :param domain: Domain name.
        :type domain: ``str``

        :param name: Full record name (fqdn).
        :type name: ``str``
        """
        if name == domain:
            # Map "root" record names to None to be consistent with other
            # drivers
            return None

        # Strip domain portion
        name = name.replace('.%s' % (domain), '')
        return name

    def _ex_connection_class_kwargs(self):
        kwargs = self.openstack_connection_kwargs()
        kwargs['ex_force_auth_url'] = 'https://region-a.geo-1.identity.hpcloudsvc.com:35357/v3/auth/tokens'

        return kwargs
