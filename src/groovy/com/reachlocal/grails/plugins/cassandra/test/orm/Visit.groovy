package com.reachlocal.grails.plugins.cassandra.test.orm

/**
 * @author: Bob Florian
 */
class Visit
{
	UUID uuid
	String siteName
	String referrerType
	String referrerName
	Date occurTime

	static cassandraMapping = [
			primaryKey: 'uuid',
			explicitIndexes: [
			        'siteName'
			]
	]
}
