-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION moonshot" to load this file. \quit

CREATE FUNCTION java_call_handler() RETURNS language_handler
    AS 'MODULE_PATHNAME'
    LANGUAGE C;

CREATE LANGUAGE MSJAVA
    HANDLER java_call_handler;

CREATE FUNCTION ms_kill_global_workers() RETURNS INT
    AS 'MODULE_PATHNAME'
    LANGUAGE C;

CREATE FUNCTION ms_kill_user_workers() RETURNS INT
    AS 'MODULE_PATHNAME'
    LANGUAGE C;

CREATE FUNCTION ms_show_user_queue() RETURNS INT
    AS 'MODULE_PATHNAME'
    LANGUAGE C;

CREATE FUNCTION ms_clear_user_queue() RETURNS INT
    AS 'MODULE_PATHNAME'
    LANGUAGE C;


