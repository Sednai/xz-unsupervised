/*
 * module-info.java
 */

module unsupervised {
	exports ai.sedn.unsupervised;

	requires java.sql;
	requires plunijava;
	requires transitive tornado.api;
}
