'''
Created on Aug 17, 2015

@author: paepcke
'''

import functools
import json
import threading

from redis_bus_python.redis_bus import BusAdapter
import wikipedia


class SchoolbusWikipedia(object):
    '''
    {"summary" : "2", "topic" : "Germany"}
    
    '''
    TOPIC = 'wikipedia'

    def __init__(self):
        '''
        Constructor
        '''
        self.bus = BusAdapter()
        self.bus.subscribeToTopic(SchoolbusWikipedia.TOPIC, 
                                  functools.partial(self.get_info_handler))
        
        # Hang till keyboard_interrupt:
        try:
            self.exit_event = threading.Event().wait()
        except KeyboardInterrupt:
            print('Exiting wikipedia module.')
        
    
    def summary(self, term, num_sentences=1):
        return wikipedia.summary(term, sentences=num_sentences)
    
    def page(self, term):
        return wikipedia.search(term, results=1)
    
    def geosearch(self, lat, longitude, num_results=1, radius=1000):
        return wikipedia.geosearch(lat, longitude, results=num_results, radius=radius)
    
    
    def get_info_handler(self, bus_message):
        '''
        {'topic'   : <keyword>,
         'summary' : <numSentences>,
         'geosearch' : {'lat'    : <float>,
                        'long'   : <float>
                        'radius' : <int>
                        },
         'coordinates' : 'True',
         'references'  : 'True'
         }
         
        :param bus_message:
        :type bus_message:
        '''
        #print(bus_message.content)
        try:
            req_dict = json.loads(bus_message.content)
        except ValueError:
            err_resp = {'error' : 'Bad json in wikipedia request: %s' % str(bus_message.content)}
            resp = self.bus.makeResponseMsg(bus_message, 
                                            json.dumps(err_resp))
            self.bus.publish(resp)
            return
        
        try:
            self.check_req_correctness(req_dict)
        except ValueError as e:
            err_resp = {'error' : '%s' % `e`}
            resp = self.bus.makeResponseMsg(bus_message, json.dumps(err_resp))
            self.bus.publish(resp)
            
        res_dict = {}
        
        if req_dict.get('summary', None) is not None:
            summary = wikipedia.summary(req_dict['topic'], 
                                        sentences=req_dict['summary'])
            res_dict['summary'] = summary.encode('UTF-8', 'replace')
            wants_page = False
        else:
            # Wants whole page content:
            wants_page = True
            
        if req_dict.get('geosearch', None) is not None:
            geo_parms = req_dict['geosearch'] 
            lat = geo_parms['lat']
            longitude = geo_parms['long']
            radius = geo_parms['radius']
            
            res_dict['geosearch'] = wikipedia.geosearch(lat, longitude, req_dict['topic'], radius)
        
        # Remaining request possibilities require the page to be obtained,
        # even if summary was requested:
        page = None
        
        if req_dict.get(u'coordinates', None) is not None and req_dict['coordinates']:
            if page is None:
                page = wikipedia.page(req_dict['topic'])
            try:
                (lat, longitude) = page.coordinates
                res_dict['coordinates'] = '{"lat" : "%s", "long" : "%s"}' %\
                                            (str(lat), str(longitude))
            except KeyError:
                # Wikipedia entry has not coordinates associated with it:
                res_dict['coordinates'] = '"None"'
            
        if req_dict.get('references', None) is not None and req_dict['references']:
            if page is None:
                page = wikipedia.page(req_dict['topic'])
            res_dict['references'] = page.coordinates

        if wants_page:
            if page is None:
                page = wikipedia.page(req_dict['topic'])
            res_dict['content'] = page.content
            
        resp_msg = self.bus.makeResponseMsg(bus_message, json.dumps(res_dict))
        self.bus.publish(resp_msg)
                     
    def check_req_correctness(self, req_dict):
        if req_dict.get('topic', None) is None:
            raise ValueError('No topic supplied in wikipedia request.')

        if req_dict.get('summary', None) is not None:
            # Must have numSentences as int:
            num_sentences = req_dict['summary']

            try:
                num_sentences = int(num_sentences)
            except ValueError:
                raise ValueError('Summary request must have a positive integer indicating number of sentences requested; was %s' %\
                                 str(num_sentences))
                                 
            if num_sentences < 1:
                raise ValueError('Summary request must have a positive integer indicating number of sentences requested; was %s' %\
                                  str(num_sentences))
                
        if req_dict.get('geosearch', None) is not None:
            geo_parms = req_dict['geosearch']
            try:
                if type(geo_parms['lat'])  != float or\
                   type(geo_parms['long']) != float or\
                   type(geo_parms['radius']) != int or\
                   geo_parms['radius'] < 1:
                    raise ValueError('Bad parameters to wikipedia geo search (lat/long must be floats; radius must be positive int):' %\
                                     str(geo_parms))
            except KeyError:
                raise ValueError('Missing parameter to wikipedia geo search; must have lat/long/radius')
        
        if req_dict.get('coordinates', None) is not None:
            coords_wanted = str(req_dict['coordinates']).lower()
            if coords_wanted != 'true' and coords_wanted != 'false':
                raise ValueError("Request for wikipedia topic coordinates must be 'true', or 'false', not %s" %\
                                 str(coords_wanted))
            # Normalize value to bool:
            req_dict['coordinates'] = True if coords_wanted == 'true' else False  

        if req_dict.get('references', None) is not None:
            refs_wanted = str(req_dict['references']).lower()
            if refs_wanted != 'true' and refs_wanted != 'false':
                raise ValueError("Request for wikipedia topic reference links must be 'true', or 'false', not %s" %\
                                 str(coords_wanted))
            # Normalize to bool:
            req_dict['references'] = True if refs_wanted == 'true' else False                  
    
if __name__ == '__main__':

    print('Starting wikipedia module on topic %s.' % SchoolbusWikipedia.TOPIC)
    service = SchoolbusWikipedia()
    