package com.reachlocal.grails.plugins.cassandra.test

import com.reachlocal.grails.plugins.cassandra.test.orm.User
import com.reachlocal.grails.plugins.cassandra.test.orm.Color
import org.junit.Test
import static org.junit.Assert.*
import com.reachlocal.grails.plugins.cassandra.mapping.CassandraMappingException
import com.reachlocal.grails.plugins.cassandra.test.orm.UserGroup

/**
 * @author: Bob Florian
 */
class DefaultMaxTests extends OrmTestCase
{
	@Test
	void testSetup()
	{
		initialize()
		def group = new UserGroup(uuid: "group-default-1").save()
		for (i in 1..5010) {
			def u = new User(uuid: UUID.randomUUID().toString(), city: "Reston")
			group.addToUsers(u)
		}
	}

	void testListExplicit()
	{
		def users = User.list(max:5005)
		assertEquals 5005, users.size()
	}

	void testListDefault()
	{
		def exception = false
		try {
			def users = User.list()
		}
		catch (CassandraMappingException e) {
			println e
			exception = true
		}
		assertTrue exception
	}

	void testHasManyExplicit()
	{
		def group = UserGroup.get("group-default-1")
		assertEquals 5005, group.users(max:5005).size()
	}

	void testHasManyDefault()
	{
		def group = UserGroup.get("group-default-1")
		def exception = false
		try {
			def users = group.users
		}
		catch (CassandraMappingException e) {
			println e
			exception = true
		}
		assertTrue exception
	}

	void testFindByExplicit()
	{
		def users = User.findAllByCity("Reston", [max: 5008])
		assertEquals 5008, users.size()
	}

	void testFindByDefault()
	{
		def exception = false
		try {
			def users = User.findAllByCity("Reston")
		}
		catch (CassandraMappingException e) {
			println e
			exception = true
		}
		assertTrue exception
	}

	void testCascadeDeleteDefault()
	{
		def exception = false
		try {
			def group = UserGroup.get("group-default-1")
			group.delete(cascade: true)
		}
		catch (CassandraMappingException e) {
			println e
			exception = true
		}
		assertTrue exception
	}

	void testCascadeDeleteExplicit()
	{
		def group = UserGroup.get("group-default-1")
		group.delete(cascade: true, max: 5100)
		assertEquals 0, User.list().size()
	}
}
