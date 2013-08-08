package com.reachlocal.grails.plugins.cassandra.mapping

/**
 * @author: Bob Florian
 */
class Traverser
{
	def object
	def args

	def propertyMissing(String name)
	{
		this.node = new TraverserNode(traverser: this, name: name)
	}

	def methodMissing(String name, args)
	{
		this.node = new TraverserNode(traverser: this, name: name, args: args)
	}

	private TraverserNode node
}
