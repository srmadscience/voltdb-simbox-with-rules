

load classes ../jars/voltdb-rules.jar;
load classes ../jars/voltdb-simbox.jar;

file -inlinebatch END_OF_BATCH


CREATE TABLE volt_rules
(RULESET_NAME varchar(30) not null
,SEQNO bigint not null
,ISAND varchar(5) not null
,STACK_NAME varchar(80) not null
,RULE_FIELD varchar(80) not null
,RULE_OPERATOR varchar(2) not null
,THRESHOLD_FLOAT float 
,THRESHOLD_STRING  varchar(256) 
,THRESHOLD_EXPRESSION  varchar(256) 
,primary key (RULESET_NAME, SEQNO));


CREATE FUNCTION add_new_cell FROM METHOD simbox.CellHistoryAsStringWrangler.addNewCell;

CREATE FUNCTION get_last_n_cells FROM METHOD simbox.CellHistoryAsStringWrangler.getLastN;

--
-- Various parameters used to control system behavior
--
CREATE TABLE simbox_parameters 
(parameter_name varchar(50) not null primary key
,parameter_value bigint not null);

CREATE TABLE simbox_stats
(stat_name varchar(1024) not null primary key
,stat_value bigint not null);

CREATE table cell_table
(cell_id bigint not null primary key);

CREATE table cell_suspicious_cohorts
(cell_id bigint not null
,event_date timestamp not null
,primary key (cell_id, event_date));

CREATE table cell_suspicious_cohort_members
(cell_id bigint not null
,event_date timestamp not null
,device_id bigint not null not null
,primary key (cell_id, event_date,device_id));

CREATE INDEX cscm_ix1 ON cell_suspicious_cohort_members
(device_id,event_date);

CREATE view suspicious_devices_view AS
SELECT device_id, min(event_date) min_event_date
, max(event_date) max_event_date
, count(*) how_many 
from cell_suspicious_cohort_members
GROUP BY device_id;


CREATE table device_table
(device_id bigint not null primary key
,current_cell_id bigint 
,first_seen timestamp not null
,last_seen timestamp not null
,cell_history_as_string varchar(120) not null
,cell_history_as_string_last3 varchar(120) not null
,cell_history_as_string_last6 varchar(120) not null
,suspicious_because varchar(1024)
,suspicious_value bigint
);

PARTITION TABLE device_table ON COLUMN device_id;

CREATE INDEX dt_ix1 ON device_table (cell_history_as_string_last3,device_id);

CREATE INDEX dt_ix2 ON device_table (cell_history_as_string_last6, device_id);

CREATE INDEX dt_ix3 ON device_table (suspicious_because, device_id);

CREATE VIEW suspicious_totals_view AS
SELECT suspicious_because, count(*) how_many
FROM device_table
WHERE suspicious_because IS NOT NULL
GROUP BY suspicious_because;

CREATE VIEW last_3_cells AS
SELECT cell_history_as_string_last3
     , count(*) how_many
FROM device_table
GROUP BY cell_history_as_string_last3;

CREATE INDEX l3c_ix1 ON last_3_cells (how_many) 
;
     
CREATE VIEW last_6_cells AS
SELECT cell_history_as_string_last6
     , count(*) how_many
FROM device_table
GROUP BY cell_history_as_string_last6;

CREATE INDEX l6c_ix1 ON last_6_cells (how_many) ;

CREATE table device_cell_history
(device_id bigint not null 
,current_cell_id bigint 
,from_timestamp timestamp not null
,to_timestamp timestamp not null
,incoming_call_count bigint default 0 not null
,outgoing_call_count bigint default 0 not null
,incoming_call_duration bigint default 0 not null
,outgoing_call_duration bigint default 0 not null
,suspicious_because varchar(120)
,primary key (device_id,from_timestamp));


PARTITION TABLE device_cell_history ON COLUMN device_id;

CREATE INDEX dch_ix1 ON device_cell_history (device_id, to_timestamp);

CREATE table device_incoming_call_history
(device_id bigint not null 
,other_number  bigint not null 
,cell_id bigint not null
,start_time timestamp not null
,end_time timestamp not null
,duration int not null
,status_code varchar(1) not null
,primary key (device_id,start_time)
);

PARTITION TABLE device_incoming_call_history ON COLUMN device_id;



CREATE table device_outgoing_call_history
(device_id bigint not null 
,other_number  bigint not null 
,cell_id bigint not null
,start_time timestamp not null
,end_time timestamp not null
,duration int not null
,status_code varchar(1) not null
,primary key (device_id,start_time)
);

PARTITION TABLE device_outgoing_call_history ON COLUMN device_id;



CREATE PROCEDURE 
   PARTITION ON TABLE device_table COLUMN device_id
   FROM CLASS simbox.ReportCellChange;  
   
CREATE PROCEDURE 
   PARTITION ON TABLE device_table COLUMN device_id
   FROM CLASS simbox.RegisterDevice;  
   
    
CREATE PROCEDURE 
   PARTITION ON TABLE device_table COLUMN device_id
   FROM CLASS simbox.ReportDeviceActivity;       
   
CREATE PROCEDURE 
   PARTITION ON TABLE device_table COLUMN device_id
   FROM CLASS simbox.GetDevice;
   
CREATE PROCEDURE 
   FROM CLASS simbox.NoteSuspiciousCohort;       
   
create procedure getSimboxDeviceStatus as 
select suspicious_because
     , count(*) how_many
from device_table 
where device_id in ?
group by suspicious_because
order by suspicious_because;

create procedure getSuspectedDeviceSummary AS
select suspicious_because, how_many  
from suspicious_totals_view
order by how_many desc;

create procedure clearStats AS
UPDATE simbox_stats SET stat_value = 0;

CREATE procedure GetPartition6CellRuns 
DIRECTED 
AS
select cell_history_as_string_last6
     , how_many  
from last_6_cells
order by how_many desc limit 3;

CREATE procedure GetPartition3CellRuns 
DIRECTED 
AS
select cell_history_as_string_last3
     , how_many  
from last_3_cells
order by how_many desc limit 1;

CREATE PROCEDURE ShowSimboxActivity__promBL AS
BEGIN
--
select 'simbox_parameter_'||parameter_name statname
     ,  'simbox_parameter_'||parameter_name stathelp  
     , parameter_value statvalue 
from simbox_parameters order by parameter_name;
--
select 'simbox_stats_'||stat_name statname
     ,  'simbox_stats_'||stat_name stathelp  
     , stat_value statvalue 
from simbox_stats order by stat_name;
--
END;



END_OF_BATCH


--
-- These parameters can be changed while the system is running
--
upsert into simbox_parameters
(parameter_name,parameter_value)
VALUES
('OUTGOING_CALL_ONLY_COUNT',10);

upsert into simbox_parameters
(parameter_name,parameter_value)
VALUES
('OUTGOING_INCOMING_RATIO',10);

upsert into simbox_parameters
(parameter_name,parameter_value)
VALUES
('NOT_NEW_ANY_MORE_DAYS',10);

upsert into simbox_parameters
(parameter_name,parameter_value)
VALUES
('BUSYNESS_PERCENTAGE',10);

upsert into simbox_parameters
(parameter_name,parameter_value)
VALUES
('TOP_N',10);

upsert into simbox_parameters
(parameter_name,parameter_value)
VALUES
('TOP_BOTTOM_N_RATIO',10);

upsert into simbox_parameters
(parameter_name,parameter_value)
VALUES
('ENABLE_SUSPICOUS_COHORT_DETECTION',0);

upsert into simbox_parameters
(parameter_name,parameter_value)
VALUES
('SIMBOX_CALLS_ITSELF',0);

--
-- We create values for all stats so prometheus works properly...
--

UPSERT INTO simbox_stats VALUES ('simboxstatus_not_suspected',0);
UPSERT INTO simbox_stats VALUES ('simboxstatus_some_incoming_calls_from_known_bad_numbers',0);
UPSERT INTO simbox_stats VALUES ('simboxstatus_suspicious_device_has_no_incoming_calls',0);
UPSERT INTO simbox_stats VALUES ('simboxstatus_suspiciously_moving_device',0);
UPSERT INTO simbox_stats VALUES ('simboxstatus_total_incoming_outgoing_ratio_bad',0);
UPSERT INTO simbox_stats VALUES ('simboxstatus_topn_incoming_outgoing_ratio_bad',0);

UPSERT INTO simbox_stats VALUES ('suspicious_because_some_incoming_calls_from_known_bad_numbers',0);
UPSERT INTO simbox_stats VALUES ('suspicious_because_suspicious_device_has_no_incoming_calls',0);
UPSERT INTO simbox_stats VALUES ('suspicious_because_suspiciously_moving_device',0);
UPSERT INTO simbox_stats VALUES ('suspicious_because_all_incoming_calls_from_known_bad_numbers',0);
UPSERT INTO simbox_stats VALUES ('suspicious_because_total_incoming_outgoing_ratio_bad',0);
UPSERT INTO simbox_stats VALUES ('suspicious_because_topn_incoming_outgoing_ratio_bad',0);                                                                                                          


INSERT INTO volt_rules
VALUES
('SIMBOX',1,'AND','all_incoming_calls_from_known_bad_numbers', 'thisDeviceIsSuspicious','=',1,null,null);

INSERT INTO volt_rules
VALUES
('SIMBOX',2,'AND','all_incoming_calls_from_known_bad_numbers', 'actualBusyInCallPct','>=',1,null,null);

INSERT INTO volt_rules
VALUES
('SIMBOX',3,'AND','all_incoming_calls_from_known_bad_numbers', 'actualBusyInCallSuspicuousPct','=',null,null,'actualBusyInCallPct');



INSERT INTO volt_rules
VALUES
('SIMBOX',21,'AND','some_incoming_calls_from_known_bad_numbers', 'thisDeviceIsSuspicious','=',1,null,null);

INSERT INTO volt_rules
VALUES
('SIMBOX',22,'AND','some_incoming_calls_from_known_bad_numbers', 'actualBusyInCallSuspicuousPct','>',1,null,null);



INSERT INTO volt_rules
VALUES
('SIMBOX',31,'AND','suspicious_device_has_no_incoming_calls', 'thisDeviceIsSuspicious','=',1,null,null);

INSERT INTO volt_rules
VALUES
('SIMBOX',32,'AND','suspicious_device_has_no_incoming_calls', 'incomingCallCount','=',0,null,null);

INSERT INTO volt_rules
VALUES
('SIMBOX',33,'AND','suspicious_device_has_no_incoming_calls', 'outgoingCallCount','>',0,null,null);


INSERT INTO volt_rules
VALUES
('SIMBOX',41,'AND','suspiciously_moving_device', 'thisDeviceIsSuspicious','=',1,null,null);






INSERT INTO volt_rules
VALUES
('SIMBOX',51,'AND','total_incoming_outgoing_ratio_bad', 'actualBusynessPercentage','>=',null,null,'busynessPercentage');

INSERT INTO volt_rules
VALUES
('SIMBOX',52,'AND','total_incoming_outgoing_ratio_bad', 'outgoingIncomingRatioTrip','<',null,null,'outgoingCallCount');





INSERT INTO volt_rules
VALUES
('SIMBOX',61,'AND','topn_incoming_outgoing_ratio_bad', 'actualBusynessPercentage','>=',null,null,'busynessPercentage');

INSERT INTO volt_rules
VALUES
('SIMBOX',62,'AND','topn_incoming_outgoing_ratio_bad', 'outCallTopBottomNRatio','<',null,null,'topBottomNRatio');






