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

package com.reachlocal.grails.plugins.cassandra.test;

import com.reachlocal.grails.plugins.cassandra.test.orm.User
import com.reachlocal.grails.plugins.cassandra.test.orm.UserGroup
import com.reachlocal.grails.plugins.cassandra.test.orm.UserGroupMeeting
import java.text.SimpleDateFormat;


/**
 * @author: Bob Florian
 */
public class ClassMethodsTests extends OrmTestCase
{
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
				birthDate:  DAY_FORMAT.parse('1980-03-15')).save()

		new User(
				uuid: "x2xx-xxxx-xxxx-xxxx",
				name: "Get Test 2",
				email: "email2@local.com",
				state:  "VA", phone: '301-555-1212',
				gender: 'Female',
				city: 'Reston',
				birthDate:  DAY_FORMAT.parse('1985-09-14')).save()

		new User(
				uuid: "x3xx-xxxx-xxxx-xxxx",
				name: "Get Test 3",
				state:  "MD",
				phone: '301-555-1234',
				gender: 'Female',
				city:  'Ellicott City',
				birthDate:  DAY_FORMAT.parse('1976-07-04')).save()

		new User(
				uuid: "x4xx-xxxx-xxxx-xxxx",
				name: "Get Test 4",
				state:  "CA",
				phone: '301-555-1111',
				gender: 'Female',
				city:  'Pleasanton',
				birthDate:  DAY_FORMAT.parse('1962-06-10')).save()

		new User(
				uuid: "x5xx-xxxx-xxxx-xxxx",
				name: "Get Test 5",
				state:  "MD",
				phone: '301-555-1212',
				gender: 'Male',
				city:  'Olney',
				birthDate:  DAY_FORMAT.parse('1991-11-12')).save()

		persistence.printClear()

		println "\n--- getCassandra() ---"
		persistence.printClear()
		assertEquals client, User.cassandra

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

		println "\n--- list() ---"
		r = User.list()
		persistence.printClear()
		println r
		assertEquals 5, r.size()

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

		println "\n--- countByGender('Male') [explicit TO BE IMPLEMENTED] ---"
		r = User.countByGender('Male')
		persistence.printClear()
		println r

		println "\n--- countByCityAndGender('Ellicott City','Male',[max: 20, reversed:  true]) [explicit] ---"
		r = User.countByCityAndGender('Ellicott City','Male',[max: 20, reversed:  true])
		persistence.printClear()
		println r
		assertEquals 1, r

		println "\n--- User.getCounts(by: ['birthDate']) ---"
		r = User.getCounts(by: ['birthDate'])
		persistence.printClear()
		println r

		println "\n--- User.getCounts(by: 'birthDate', start: '1978-01-01', finish: '1985-12-31') ---"
		r = User.getCounts(by: 'birthDate', start: '1975-01-01', finish: '1984-12-31')
		persistence.printClear()
		println r

		println "\n--- User.getCounts(by: 'birthDate', start: '1978-01-01', finish: '1985-12-31') ---"
		r = User.getCounts(by: 'birthDate', start: DAY_FORMAT.parse('1977-01-01'), finish: DAY_FORMAT.parse('1984-12-31'))
		persistence.printClear()
		println r

		println "\n--- User.getCounts(where: [gender: 'Male', by: 'birthDate']) ---"
		r = User.getCounts(where: [gender: 'Male'], by: 'birthDate')
		persistence.printClear()
		println r
		assertEquals 2, r.size()
		assertEquals 1, r['1980-03-15T00']

		println "\n--- User.getCounts(where: [gender: 'Female'], by: ['birthDate','city']) ---"
		r = User.getCounts(where: [gender: 'Female'], by: ['birthDate','city'])
		persistence.printClear()
		println r

		println "\n--- User.getCounts(by: ['birthDate','state']) ---"
		r = User.getCounts(by: ['birthDate','state'])
		persistence.printClear()
		println r

		println "\n--- User.getCounts(by: ['birthDate','state'], groupBy: 'state') ---"
		r = User.getCounts(by: ['birthDate','state'], groupBy: 'state')
		persistence.printClear()
		println r

		println "\n--- User.getCounts(grouped: ['birthDate','state']) ---"
		try {
			r = User.getCounts(grouped: ['birthDate','state'])
			fail("Illegal argument exception now thrown when by not specified for getCounts")
		}
		catch (IllegalArgumentException e) {

		}

		println "\n--- User.getCountsGroupedByBirthDate(where: [gender: 'Male']) ---"
		r = User.getCountsByBirthDate(where: [gender: 'Male'])
		persistence.printClear()
		println r

		println "\n--- User.getCountsGroupedByBirthDate() ---"
		r = User.getCountsByBirthDate()
		persistence.printClear()
		println r

		println "\n--- User.getCountsGroupedByBirthDate() ---"
		r = User.getCountsByBirthDateTotal()
		persistence.printClear()
		println r
		assertEquals 5, r

		println "\n--- User.getCountsByBirthDateAndCity() ---"
		r = User.getCountsByBirthDateAndCity(where: [gender: 'Female'])
		persistence.printClear()
		println r

		println "\n--- User.getCountsByBirthDateAndCityTotal() ---"
		r = User.getCountsByBirthDateAndCityTotal(where: [gender: 'Female'])
		persistence.printClear()
		println r
		assertEquals 3, r

		println "\n--- User.getCountsByBirthDateAndCity() ---"
		r = User.getCountsByBirthDateAndCity(where: [gender: 'Female'], dateFormat: new SimpleDateFormat("yyyy"))
		persistence.printClear()
		println r

		println "\n--- User.getCountsByBirthDateAndCityGroupByCity() ---"
		r = User.getCountsByBirthDateAndCityGroupByCity(where: [gender: 'Female'])
		persistence.printClear()
		println r

		println "\n--- User.getCountsByBirthDateAndCityGroupByCity() ---"
		r = User.getCountsByBirthDateAndCityGroupByBirthDate(where: [gender: 'Female'], dateFormat: new SimpleDateFormat("yyyy"))
		persistence.printClear()
		println r
	}

	static protected final DAY_FORMAT = new SimpleDateFormat("yyyy-MM-dd")
}
