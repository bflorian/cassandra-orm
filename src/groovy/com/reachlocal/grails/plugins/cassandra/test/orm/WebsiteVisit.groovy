package com.reachlocal.grails.plugins.cassandra.test.orm

/**
 * @author: Bob Florian
 */
class WebsiteVisit
{
	// Set by client
	//
	UUID visitId             // Time-based UUID unique to this visit (i.e.    sequence of page views in a session)
	UUID visitorId           // Globally unique identified of this visitor (i.e. person, as far as can be determined)
	String etsSiteId         // Globally unique identifier of this advertiser site
	String gmaid             // Global Master Advertiser ID (platform concatenated with master advertiser ID, derived from widget ID)
	String mcid              // Master campaign ID (derived from Sub-campaign ID)
	String cid               // Campaign ID (derived from sub-campaign ID)
	String scid              // Sub-campaign ID
	Boolean newVisitor       // True for first visit by this visitor, otherwise false
	Date   occurTime         // Time of HTTP request (msec since 1/1/1970)
	String pageUrl           // URL of the requested page (i.e. document.URL)
	String pageTitle         // Title of the requested page (i.e. document.title) or the URL if title not set
	String userAgent         // The HTTP User-Agent header value
	String language          // The HTTP Accept-Language header value
	Double geoLat            // Latitude
	Double geoLng            // Longitude
	String geoZip            // Zip or postal code
	String geoCity           // Nearest city, i.e. Baltimore
	String geoRegion         // State or province, i.e. MD
	String geoCountry        // Country code, i.e. USA, CAN, ...
	String refUrl            // Value of Referer HTTP header param
	String refClass          // ReachLocal | Organic
	String refType           // Direct | Search | Directory | Social | Other Website
	String refName           // Name of know sites (i.e. Google, Bing) or domain name
	String refKeyword        // Search expression (set for refType == Search only)

	// Needed eventually but not for phase 1
	//
	String refCtype = "Organic" // Product name (ReachSearch | ReachDispay | ReachRemarketing | ReachCast | Organic (for non-reachlocal))
	String refPubName = "none"  // Publisher name (derived from sub-campaign ID) or 'none' for non-reachlocal

	// Set by API prior to visit event insertion
	//
	String refPage           // Same as refUrl, but not set for searches

	// Session properties (set after visit event insertion)
	//
	Boolean bounced = true   // True if no subsequent page views occured within session timeout
	Long timeOnSite = 0      // Occur time of last action during this visit site during that session
	Integer totalActions = 1 // Total number of page views and posts for the visit

	Map props

	static cassandraMapping = [
			cluster: 'etsData',
			keySpace: 'ets',
			expandoMap: 'props',

			unindexedPrimaryKey:
			//primaryKey:
					'visitId',

			explicitIndexes: [
					['gmaid'],
					['etsSiteId'],
					['visitorId'],
					['etsSiteId','refClass','refType'],
			],

			counters: [
					[findBy: ['gmaid'], groupBy: ['occurTime','refClass','refCtype','refType','newVisitor','bounced']],
					[findBy: ['etsSiteId'], groupBy: ['occurTime','refClass','refCtype','refType','newVisitor','bounced']],
					[findBy: ['etsSiteId'], groupBy: ['occurTime','refClass','refCtype','refType','newVisitor','bounced','refPubName','refName']],
					[findBy: ['etsSiteId'], groupBy: ['occurTime','refClass','refCtype','refType','newVisitor','bounced','refPubName','refName','refKeyword']],
					[findBy: ['etsSiteId'], groupBy: ['occurTime','refClass','refCtype','refType','newVisitor','bounced','refPubName','refName','refPage']],
					[findBy: ['etsSiteId'], groupBy: ['occurTime','refClass','refCtype','refType','newVisitor','bounced','pageUrl']],
					[findBy: ['etsSiteId'], groupBy: ['occurTime','refClass','refCtype','refType','newVisitor','bounced','pageTitle']],

					[findBy: ['etsSiteId'], groupBy: ['occurTime']],
					[findBy: ['etsSiteId'], groupBy: ['occurTime','newVisitor']],
					[findBy: ['etsSiteId'], groupBy: ['occurTime','bounced']]
			]
	]
}
