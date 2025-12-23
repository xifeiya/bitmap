-- Bitmap index handler function
CREATE FUNCTION bmhandler(internal)
    RETURNS index_am_handler
    AS 'MODULE_PATHNAME', 'bmhandler'
    LANGUAGE C STRICT;

-- Register bitmap index access method
CREATE ACCESS METHOD yabit TYPE INDEX HANDLER bmhandler;
COMMENT ON ACCESS METHOD yabit IS 'bitmap index access method';

-- Register bitmap index operator class (for integer types)
CREATE OPERATOR CLASS int4_ops DEFAULT
    FOR TYPE integer USING yabit AS
    OPERATOR        1       <,
    OPERATOR        2       <=,
    OPERATOR        3       =,
    OPERATOR        4       >=,
    OPERATOR        5       >,
    FUNCTION 1 btint4cmp(integer, integer);

CREATE OPERATOR CLASS numeric_ops DEFAULT
    FOR TYPE numeric USING yabit AS
    OPERATOR        1       <,
    OPERATOR        2       <=,
    OPERATOR        3       =,
    OPERATOR        4       >=,
    OPERATOR        5       >,
    FUNCTION        1       numeric_cmp(numeric,numeric);

CREATE OPERATOR CLASS date_ops DEFAULT
FOR TYPE date USING yabit AS
    OPERATOR        1       <,
    OPERATOR        2       <=,
    OPERATOR        3       =,
    OPERATOR        4       >=,
    OPERATOR        5       >,
    FUNCTION 1 date_cmp(date, date);


CREATE FUNCTION tpch_q6(text, text, text) RETURNS void
    AS 'MODULE_PATHNAME', 'tpch_q6'
    LANGUAGE C STRICT;

-- Function to view detailed information of an iov item
CREATE FUNCTION iovitemdetail(text, int4, int4) RETURNS text
    AS 'MODULE_PATHNAME', 'iovitemdetail'
    LANGUAGE C STRICT;

-- Add comments to the iovitemdetail function
COMMENT ON FUNCTION iovitemdetail(text, int4, int4) IS 'View detailed information about an IOV item in a bitmap index';