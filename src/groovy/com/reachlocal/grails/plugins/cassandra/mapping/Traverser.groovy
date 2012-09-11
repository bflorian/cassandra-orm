package com.reachlocal.grails.plugins.cassandra.mapping

/**
 * @author: Bob Florian
 */
class Traverser 
{
	public Traverser(object, args=null)
	{
		this.object = object
		this.args = args
	}

	def propertyMissing(String name)
	{
		this.node = new TraverserNode(traverser: this, name: name)
	}

	def methodMissing(String name, args)
	{
		this.node = new TraverserNode(traverser: this, name: name, args: args)
	}

	TraverserNode node
	def object
	def args
}
