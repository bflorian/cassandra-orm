/*
 * Copyright 2012 ReachLocal Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.reachlocal.grails.plugins.cassandra.test

import java.text.SimpleDateFormat

import org.junit.Test

import com.reachlocal.grails.plugins.cassandra.test.orm.Color
import com.reachlocal.grails.plugins.cassandra.test.orm.User
import com.reachlocal.grails.plugins.cassandra.test.orm.UserGroup
import com.reachlocal.grails.plugins.cassandra.test.orm.UserGroupMeeting
import com.reachlocal.grails.plugins.cassandra.test.orm.Visit

/**
 * @author: Bob Florian
 */
public class ClassMethodsTests extends OrmTestCase
{
	@Test
	void testAll()
	{
		initialize()

		println "-- setup --"
		new User(
				uuid: "x1xx-xxxx-xxxx-xxxx",
				name: "Get Test",
				state:  "MD",
				phone: '301-555-1212',
				gender: 'Male',
				city:  'Ellicott City',
				favoriteColor: Color.BLUE,
				favoriteSports: ['Skiing','Cycling','Sailing'],
				birthDate:  DAY_FORMAT.parse('1980-03-15')).save()

		new User(
				uuid: "x2xx-xxxx-xxxx-xxxx",
				name: "Get Test 2",
				email: "email2@local.com",
				state:  "VA", phone: '301-555-1212',
				gender: 'Female',
				city: 'Reston',
				favoriteColor: Color.RED,
				birthDate:  DAY_FORMAT.parse('1985-09-14')).save(consistencyLevel: "CL_LOCAL_QUORUM")

		new User(
				uuid: "x3xx-xxxx-xxxx-xxxx",
				name: "Get Test 3",
				state:  "MD",
				phone: '301-555-1234',
				gender: 'Female',
				city:  'Ellicott City',
				favoriteColor: Color.GREEN,
				birthDate:  DAY_FORMAT.parse('1976-07-04')).save()

		new User(
				uuid: "x4xx-xxxx-xxxx-xxxx",
				name: "Get Test 4",
				state:  "CA",
				phone: '301-555-1111',
				gender: 'Female',
				city:  'Pleasanton',
				favoriteColor: Color.YELLOW,
				birthDate:  DAY_FORMAT.parse('1962-06-10')).save()

		new User(
				uuid: "x5xx-xxxx-xxxx-xxxx",
				name: "Get Test 5",
				state:  "MD",
				phone: '301-555-1212',
				gender: 'Male',
				city:  'Olney',
				favoriteColor: Color.ORANGE ,
				birthDate:  DAY_FORMAT.parse('1991-11-12')).save()

		// TODO - cluster parameter support temporarily removed
		/*
		new User(
				uuid: "x2xx-xxxx-xxxx-2222",
				name: "Get Test 2",
				email: "email2@local.com",
				state:  "VA",
				phone: '301-555-1212',
				gender: 'Female',
				city: 'Reston',
				favoriteColor: Color.RED,
				birthDate:  DAY_FORMAT.parse('1985-09-14')).save(cluster: "mockCluster2")
        */

		def v1 = new Visit(
				siteName : "SITE1",
				referrerType: "Search",
				referrerName: "Google",
				occurTime: new Date()
		).save()

		def v2 = new Visit(
				siteName : "SITE1",
				referrerType: "Search",
				referrerName: "Bing",
				occurTime: new Date()
		).save()

		def v3 = new Visit(
				siteName : "SITE1",
				referrerType: "Facebook",
				referrerName: "Social",
				occurTime: new Date()
		).save()

		persistence.printClear()

		println "\n--- getCassandra() ---"
		persistence.printClear()
		assertEquals client, User.cassandra

		println "\n--- getCassandraCluster() ---"
		persistence.printClear()
		assertEquals 'mockCluster', User.cassandraCluster

		println "\n--- getKeySpace() ---"
		persistence.printClear()
		assertEquals "mock", User.keySpace
		assertEquals "mockDefault", UserGroup.keySpace

		println "\n--- getColumnFamilyName() ---"
		persistence.printClear()
		assertEquals "MockUser", User.columnFamilyName
		assertEquals "UserGroup", UserGroup.columnFamilyName

		println "\n--- getColumnFamily() ---"
		persistence.printClear()
		assertEquals "MockUser_CFO", User.columnFamily
		assertEquals "UserGroup_CFO", UserGroup.columnFamily

		println "\n--- getIndexColumnFamily() ---"
		assertEquals "MockUser_IDX_CFO", User.indexColumnFamily
		assertEquals "UserGroup_IDX_CFO", UserGroup.indexColumnFamily
		persistence.printClear()

		println "\n--- getCounterColumnFamily() ---"
		assertEquals "MockUser_CTR_CFO", User.counterColumnFamily
		assertEquals "UserGroup_CTR_CFO", UserGroup.counterColumnFamily
		persistence.printClear()

		println "\n--- belongsToClass(clazz) ---"
		persistence.printClear()
		assertTrue User.belongsToClass(UserGroup)
		assertFalse User.belongsToClass(UserGroupMeeting)

		println "\n--- get() ---"
		def r = User.get("x1xx-xxxx-xxxx-xxxx")
		persistence.printClear()
		println r
		assertEquals "Get Test", r.name
		assertEquals 3, r.favoriteSports.size()
		assertEquals Color.BLUE, r.favoriteColor
		assertEquals "mockCluster", r.cassandraCluster

		// TODO - cluster parameter support temporarily removed
		/*
		println "\n--- get('x2xx-xxxx-xxxx-xxxx', [cluster: 'mockCluster2']) ---"
		r = User.get("x2xx-xxxx-xxxx-xxxx", [cluster: "mockCluster2"])
		println r
		assertNull r

		println "\n--- get('x2xx-xxxx-xxxx-2222', [cluster: 'mockCluster2']) ---"
		r = User.get("x2xx-xxxx-xxxx-2222", [cluster: "mockCluster2"])
		persistence.printClear()
		println r
		assertEquals "mockCluster2", r.cassandraCluster
        */

		println "\n--- getAll(['x1xx-xxxx-xxxx-xxxx','x2xx-xxxx-xxxx-xxxx','x3xx-xxxx-xxxx-xxxx']) ---"
		r = User.getAll(['x1xx-xxxx-xxxx-xxxx','x2xx-xxxx-xxxx-xxxx','x3xx-xxxx-xxxx-xxxx'])
		persistence.printClear()
		println r
		assertEquals 3, r.size()
		r.each {
			assertTrue it instanceof User
		}

		println "\n--- getAll(['x1xx-xxxx-xxxx-xxxx','x3xx-xxxx-xxxx-xxxx']) ---"
		r = User.getAll(['x1xx-xxxx-xxxx-xxxx','x3xx-xxxx-xxxx-xxxx'],[column:  'favoriteColor'])
		persistence.printClear()
		println r
		assertEquals 2, r.size()
		r.each {
			assertFalse it instanceof User
		}

		println "\n -- findOrCreate(), existing ---"
		r = User.findOrCreate("x5xx-xxxx-xxxx-xxxx")
		persistence.printClear()
		assertNotNull r
		assertEquals "Get Test 5", r.name
		assertEquals "mockCluster", r.cassandraCluster

		// TODO - cluster parameter support temporarily removed
		/*
		println "\n -- findOrCreate([cluster: 'mockCluster2']), existing ---"
		r = User.findOrCreate("x2xx-xxxx-xxxx-2222",[cluster: 'mockCluster2'])
		persistence.printClear()
		assertNotNull r
		assertEquals "Get Test 2", r.name
		assertEquals "mockCluster2", r.cassandraCluster
        */

		println "\n -- findOrCreate(), not existing ---"
		r = User.findOrCreate("x5xx-xxxx-yyyy-zzzz")
		persistence.printClear()
		assertNotNull r
		assertEquals "x5xx-xxxx-yyyy-zzzz", r.uuid
		assertEquals "mockCluster", r.cassandraCluster
		assertNull User.get("x5xx-xxxx-yyyy-zzzz")

		// TODO - cluster parameter support temporarily removed
		/*
		println "\n -- findOrCreate([cluster: 'mockCluster2']), not existing ---"
		r = User.findOrCreate("x5xx-xxxx-yyyy-zzzz",[cluster: 'mockCluster2'])
		assertNotNull r
		assertEquals "x5xx-xxxx-yyyy-zzzz", r.uuid
		assertEquals "mockCluster2", r.cassandraCluster
		assertNull User.get("x5xx-xxxx-yyyy-zzzz")
        */

		println "\n -- findOrSave(), not existing ---"
		r = User.findOrSave("aaaa-xxxx-yyyy-zzz1")
		assertNotNull r
		assertEquals "aaaa-xxxx-yyyy-zzz1", r.uuid
		assertNotNull User.get("aaaa-xxxx-yyyy-zzz1")

		println "\n--- list() ---"
		r = User.list()
		persistence.printClear()
		println r
		assertEquals 6, r.size()

		// TODO - cluster parameter support temporarily removed
		/*
		println "\n--- list(cluster: 'mockCluster2') ---"
		r = User.list(cluster: 'mockCluster2')
		persistence.printClear()
		println r
		assertEquals 1, r.size()
        */

		println "\n--- list(max: 2) ---"
		r = User.list(max: 2)
		persistence.printClear()
		println r
		assertEquals 2, r.size()

		println "\n--- list(start: 'x1', finish: 'x3') ---"
		r = User.list(start: 'x1', finish: 'x3')
		persistence.printClear()
		println r
		assertEquals 3, r.size()

		println "\n--- list(startAfter: 'x1', finish: 'x3') ---"
		r = User.list(startAfter: 'x1', finish: 'x3')
		persistence.printClear()
		println r
		assertEquals 2, r.size()

		println "\n--- list(startAfter: 'x5', max: 10) ---"
		r = User.list(startAfter: 'x5', max: 10)
		persistence.printClear()
		println r
		assertEquals 0, r.size()

		println "\n--- list(start: 'x4z', finish: 'x3z', reversed: true) ---"
		r = User.list(start: 'x4z', finish: 'x3z', reversed:  true) as List
		persistence.printClear()
		println r
		assertEquals 2, r.size()
		assertEquals "Get Test 4", r[0].name

		println "\n--- findAllWhere(state: 'MD') [secondary TO BE IMPLEMENTED] ---"
		r = User.findAllWhere(state: 'MD')
		persistence.printClear()
		println r

		println "\n--- findAllWhere(phone: '301-555-1212') [explicit] ---"
		r = User.findAllWhere(phone: '301-555-1212')
		persistence.printClear()
		println r
		assertEquals 3, r.size()

		// TODO - cluster parameter support temporarily removed
		/*
		println "\n--- findAllWhere(phone: '301-555-1212',[cluster: 'mockCluster2']) [explicit] ---"
		r = User.findAllWhere(phone: '301-555-1212', [cluster: 'mockCluster2'])
		persistence.printClear()
		println r
		assertEquals 1, r.size()
        */

		println "\n--- findAllWhere(city: 'Olney', gender: 'Male') [secondary TO BE IMPLEMENTED] ---"
		r = User.findAllWhere(city: 'Olney', gender: 'Male')
		persistence.printClear()
		println r

		println "\n--- findWhere(phone: '301-555-1212') [explicit] ---"
		r = User.findWhere(phone: '301-555-1234')
		persistence.printClear()
		println r
		assertNotNull r

		println "\n--- findByPhone('301-555-1111') [explicit] ---"
		r = User.findByPhone('301-555-1111')
		persistence.printClear()
		println r
		assertNotNull r

		println "\n--- findAllByPhone('301-555-1212') [explicit] ---"
		r = User.findAllByPhone('301-555-1212')
		persistence.printClear()
		println r
		assertEquals 3, r.size()

		println "\n--- findAllByPhone('301-555-1212', [start: 'x2xx-xxxx-xxxx-xxxx']) [explicit] ---"
		r = User.findAllByPhone('301-555-1212', [start: 'x2xx-xxxx-xxxx-xxxx'])
		persistence.printClear()
		println r
		assertEquals 2, r.size()

		println "\n--- findAllByPhone('301-555-1212', [startAfter: 'x2xx-xxxx-xxxx-xxxx']) [explicit] ---"
		r = User.findAllByPhone('301-555-1212', [startAfter: 'x2xx-xxxx-xxxx-xxxx'])
		persistence.printClear()
		println r
		assertEquals 1, r.size()

		println "\n--- findAllByGender('Male') [secondary TO BE IMPLEMENTED] ---"
		r = User.findAllByGender('Male')
		persistence.printClear()
		println r

		println "\n--- findAllByCityAndGender('Ellicott City','Female',[max: 20, reversed:  true]) [explicit] ---"
		r = User.findAllByCityAndGender('Ellicott City','Female',[max: 20, reversed:  true])
		persistence.printClear()
		println r
		assertEquals 1, r.size()

		println "\n--- findAllByPhone('301-555-1212',[max: 2, column: 'name']) [explicit] ---"
		r = User.findAllByPhone('301-555-1212',[max: 2, column: 'name'])
		persistence.printClear()
		println r
		assertEquals 2, r.size()

		// TODO - cluster parameter support temporarily removed
		/*
		println "\n--- findAllByPhone('301-555-1212',[max: 2, column: 'name', cluster: 'mockCluster2']) [explicit] ---"
		r = User.findAllByPhone('301-555-1212',[max: 2, column: 'name', cluster: 'mockCluster2'])
		persistence.printClear()
		println r
		assertEquals 1, r.size()
        */

		println "\n--- findAllByPhone('301-555-1111',[max: 2, rawColumn: 'name']) [explicit] ---"
		r = User.findAllByPhone('301-555-1111',[max: 2, rawColumn: 'name'])
		persistence.printClear()
		println r

		println "\n--- findAllByPhone('301-555-1111',[max: 2, columns: ['name','city']])[explicit] ---"
		r = User.findAllByPhone('301-555-1111',[max: 2, columns: ['name','city']])
		persistence.printClear()
		println r

		println "\n--- findAllByPhone('301-555-1111',[max: 2, rawColumns: ['name','city']])[explicit] ---"
		r = User.findAllByPhone('301-555-1111',[max: 2, rawColumns: ['name','city']])
		persistence.printClear()
		println r

		println "\n--- findAllByState('VA') [secondary TO BE IMPLEMENTED] ---"
		r = User.findAllByState('VA')
		persistence.printClear()
		println r

		println "\n--- findAllByGender('Female',[max: 50]) [secondary TO BE IMPLEMENTED] ---"
		r = User.findAllByGender('Female',[max: 50])
		persistence.printClear()
		println r

		println "\n--- countByPhone('301-555-1212') [explicit] ---"
		r = User.countByPhone('301-555-1212')
		persistence.printClear()
		println r
		assertEquals 3, r

		// TODO - cluster parameter support temporarily removed
		/*
		println "\n--- countByPhone('301-555-1212',[cluster: 'mockCluster2']) [explicit] ---"
		r = User.countByPhone('301-555-1212',[cluster: 'mockCluster2'])
		persistence.printClear()
		println r
		assertEquals 1, r
        */

		println "\n--- countByGender('Male') [explicit TO BE IMPLEMENTED] ---"
		r = User.countByGender('Male')
		persistence.printClear()
		println r

		println "\n--- countByCityAndGender('Ellicott City','Male',[max: 20, reversed:  true]) [explicit] ---"
		r = User.countByCityAndGender('Ellicott City','Male',[max: 20, reversed:  true])
		persistence.printClear()
		println r
		assertEquals 1, r

		println "\n--- User.getCounts(groupBy: ['birthDate']) ---"
		r = User.getCounts(groupBy: ['birthDate'])
		persistence.printClear()
		println r
		assertEquals 5, r.size()

		// TODO - cluster parameter support temporarily removed
		/*
		println "\n--- User.getCounts(groupBy: ['birthDate'], cluster: 'mockCluster2') ---"
		r = User.getCounts(groupBy: ['birthDate'], cluster: 'mockCluster2')
		persistence.printClear()
		println r
		assertEquals 1, r.size()
        */

		println "\n--- User.getCounts(by: 'birthDate', start: DAY_FORMAT.parse('1977-01-01'), finish: DAY_FORMAT.parse('1984-12-31')) ---"
		r = User.getCounts(groupBy: 'birthDate', start: DAY_FORMAT.parse('1977-01-01'), finish: DAY_FORMAT.parse('1984-12-31'))
		persistence.printClear()
		println r

		println "\n--- User.getCounts(where: [gender: 'Male', groupBy: 'birthDate']) ---"
		r = User.getCounts(where: [gender: 'Male'], groupBy: 'birthDate')
		persistence.printClear()
		println r
		assertEquals 2, r.size()
		assertEquals 1, r['1980-03-15T05']

		println "\n--- User.getCounts(where: [gender: 'Female'], groupBy: ['birthDate','city']) ---"
		r = User.getCounts(where: [gender: 'Female'], groupBy: ['birthDate','city'])
		persistence.printClear()
		println r

		println "\n--- User.getCounts(groupBy: ['birthDate','state']) ---"
		r = User.getCounts(groupBy: ['birthDate','state'])
		persistence.printClear()
		println r

		println "\n--- User.getCounts(groupBy: ['birthDate','state'], groupBy: 'state') ---"
		r = User.getCounts(groupBy: 'state')
		persistence.printClear()
		println r

		println "\n--- User.getCountsByGenderGroupByBirthDate('Male') ---"
		r = User.getCountsByGenderGroupByBirthDate('Male')
		persistence.printClear()
		println r

		println "\n--- User.getCountsGroupByBirthDate() ---"
		r = User.getCountsGroupByBirthDate()
		persistence.printClear()
		println r

		println "\n--- User.getCountsGroupByBirthDate(grain: Calendar.DAY_OF_MONTH, reversed:  true) ---"
		r = User.getCountsGroupByBirthDate(grain: Calendar.DAY_OF_MONTH, reversed:  true)
		persistence.printClear()
		println r
		def keys = r.keySet().toList()
		assertEquals '1991-11-12', keys[0]
		assertEquals '1962-06-10', keys[-1]

		println "\n--- User.getCountsGroupByBirthDate(grain: Calendar.DAY_OF_MONTH, sort: true, reversed:  true) ---"
		r = User.getCountsGroupByBirthDate(grain: Calendar.DAY_OF_MONTH, sort: true, reversed:  true)
		persistence.printClear()
		println r
		keys = r.keySet().toList()
		assertEquals '1991-11-12', keys[0]
		assertEquals '1962-06-10', keys[-1]

		println "\n--- User.getCountsTotal() ---"
		r = User.getCountsTotal()
		persistence.printClear()
		println r
		assertEquals 5, r

		println "\n--- User.getCountsGroupedByBirthDate() ---"
		r = User.getCountsGroupByBirthDateAndStateTotal()
		persistence.printClear()
		println r
		assertEquals 5, r

		// TODO - cluster parameter support temporarily removed
		/*
		println "\n--- User.getCountsGroupedByBirthDate(cluster: 'mockCluster2') ---"
		r = User.getCountsGroupByBirthDateAndStateTotal(cluster: 'mockCluster2')
		persistence.printClear()
		println r
		assertEquals 1, r
        */

		println "\n--- User.getCountsByBirthDateAndCity() ---"
		r = User.getCountsByGenderGroupByBirthDateAndCity('Female')
		persistence.printClear()
		println r

		println "\n--- User.getCountsByBirthDateAndCityTotal() ---"
		//r = User.getCountsByGenderGroupByBirthDateAndCityTotal('Female')
		r = User.getCountsByGenderTotal('Female')
		persistence.printClear()
		println r
		assertEquals 3, r

		println "\n--- User.getCountsByBirthDateAndCity() ---"
		r = User.getCountsByGenderGroupByBirthDateAndCity('Female', [grain: Calendar.YEAR])
		persistence.printClear()
		println r

		println "\n--- User.getCountsByBirthDateAndCityGroupByCity() ---"
		r = User.getCountsByGenderGroupByCity('Female')
		persistence.printClear()
		println r

		println "\n--- User.getCountsByBirthDateAndCityGroupByCity() ---"
		r = User.getCountsGroupByBirthDate(where: [gender: 'Female'], dateFormat: new SimpleDateFormat("yyyy"))
		persistence.printClear()
		println r

		println "\n--- Visit.findAllBySiteName('SITE1')"
		r = Visit.findAllBySiteName('SITE1')
		persistence.printClear()
		println r
		assertEquals 3, r.size()

		println "\n--- Visit.findAllBySiteName('SITE1', [start:  v2.uuid])"
		r = Visit.findAllBySiteName('SITE1', [start:  v2])
		persistence.printClear()
		println r
		assertEquals 2, r.size()

		println "\n--- Visit.findAllBySiteName('SITE1', [startAfter:  v2.uuid])"
		r = Visit.findAllBySiteName('SITE1', [startAfter:  v2.ident()])
		persistence.printClear()
		println r
		assertEquals 1, r.size()
	}

	static protected DAY_FORMAT = new SimpleDateFormat("yyyy-MM-dd")

	static {
		DAY_FORMAT.setTimeZone(TimeZone.getTimeZone("America/New_York"))
	}
}
