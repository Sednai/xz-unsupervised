/*
 * module-info.java
 */

module unsupervised {
	exports ai.sedn.unsupervised;

	requires java.sql;
	//requires transitive org.postgresql.pljava;
	//requires transitive org.postgresql.jdbc;
	requires transitive tornado.api;

}
