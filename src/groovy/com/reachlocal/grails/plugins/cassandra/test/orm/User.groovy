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

package com.reachlocal.grails.plugins.cassandra.test.orm

/**
 * @author: Bob Florian
 */
class User
{
	String uuid
	String email
	String name
	String city
	String state
	String phone
	String gender
	String period
	Date birthDate
	Color favoriteColor
	String username
	List favoriteSports

	UserGroup userGroup

	static belongsTo = [userGroup: UserGroup]

	static cassandraMapping = [
			keySpace: 'mock',
			cluster: 'mockCluster',
			columnFamily: 'MockUser',
			primaryKey: 'uuid',
			explicitIndexes: ["email","phone","city",["city","gender"]],
			secondaryIndexes: ["gender","state"],
			ttl: [username: 10000],
			counters: [
					[findBy: ['state'], groupBy: ['city']],
					[groupBy: ['birthDate']],
					[findBy: ['gender'], groupBy: ['birthDate']],
					[groupBy: ['birthDate','state']],
					[findBy: ['gender'], groupBy: ['birthDate','city']],
					[findBy: ['gender'], groupBy: ['birthDate','state','city']],
					[findBy: ['gender','period'], groupBy: ['birthDate','state','city']],
					[findBy: ['gender','period'], groupBy: ['birthDate','city']]
			]
	]
}

enum Color {RED, GREEN, BLUE, YELLOW, ORANGE, PURPLE}
