let moment = require('moment');
const config = require('./config');
    let lineIp = config.lineIp;
    const aggIp = config.aggIp;
    console.log('lineIp ',lineIp);
    const knex_line = require('knex') ({
            client: 'pg',
            connection: {
                    user: 'plantworks',
                    host: lineIp,
                    database: 'plantworks',
                    password: 'plantworks',
                    port: 5432,
            }
    });
  
  	const timescale_line = require('knex') ({
            client: 'pg',
            connection: {
                    user: 'plantworks',
                    host: lineIp,
                    database: 'plantworks_timescale',
                    password: 'plantworks',
                    port: 5432,
            }
    });

    const knex_agg = require('knex') ({
            client: 'pg',
            connection: {
                    user: 'plantworks',
                    host: aggIp,
                    database: 'plantworks',
                    password: 'plantworks',
                    port: 5432,
            }
    });
  
  	 const timescale_agg = require('knex') ({
            client: 'pg',
            connection: {
                    user: 'plantworks',
                    host: aggIp,
                    database: 'plantworks_timescale',
                    password: 'plantworks',
                    port: 5432,
            }
    });

    async function mainFunc() {
        let tenantID = config.tenantId;
        let machineIds = await knex_line.raw(`select id from tenant_plant_unit_machines where tenant_id = ?`, [tenantID]);
        machineIds = machineIds.rows;
        const fromDate = moment(config.startTime);
        const toDate = moment(config.endTime);
        
        async function downtime() {
            for (let i = 0; i < machineIds.length; i++) {
                let machineId = machineIds[i].id;
                let data = await knex_line.raw(`select * from tenant_plant_unit_machine_downtimes where tenant_plant_unit_machine_id = ? and start_time >= ? and start_time < ? order by start_time`,[machineId, fromDate.toDate(), toDate.toDate()]);
                data = data.rows;
                if (data.length) {
                    for (let j = 0; j < data.length; j++) {
                        let inserting_data = data[j];
                        let presentData = await knex_agg.raw(`SELECT id, start_time from tenant_plant_unit_machine_downtimes where id = ?`, [inserting_data.id]);
                        presentData = presentData.rows
                        if (!presentData.length) {
                            console.log('inserting downtimes');
                            await knex_agg.raw(`INSERT INTO tenant_plant_unit_machine_downtimes (id,tenant_id,tenant_plant_unit_machine_id,type,start_time,end_time,work_order_id,created_at,updated_at,supervisor,operator,sku,metadata) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`, [inserting_data.id, tenantID, inserting_data.tenant_plant_unit_machine_id, inserting_data.type, inserting_data.start_time, inserting_data.end_time, inserting_data.work_order_id, inserting_data.created_at, inserting_data.updated_at, inserting_data.supervisor, inserting_data.operator, inserting_data.sku, JSON.stringify(inserting_data.metadata)]);
                        } else {
                            console.log("data is present", presentData[0].start_time)
                        }
                    }
                }
            }
        }

        async function downtimeReason() {
            for (let i = 0; i < machineIds.length; i++) {
                let machineId = machineIds[i].id;
                let data = await knex_line.raw(`select * from tenant_plant_unit_machine_downtimes where tenant_plant_unit_machine_id = ? and start_time >= ? and start_time < ? order by start_time`,[machineId, fromDate.toDate(), toDate.toDate()]);
                data = data.rows
                if (data.length) {
                    for (let j = 0; j < data.length; j++) {
                        let reason_data = await knex_line.raw(`SELECT * from tenant_plant_unit_machine_downtime_reasons where tenant_plant_unit_machine_downtime_id = ?`, [data[j].id]);
                        reason_data = reason_data.rows
                        for(let k=0;k<reason_data.length;k++)
                        {
                            let inserting_data = reason_data[k];
                            let presentData = await knex_agg.raw(`SELECT id,reason_duration_in_min from tenant_plant_unit_machine_downtime_reasons where id = ?`, [inserting_data.id]);
                            presentData = presentData.rows;
                            if (!presentData.length) {
                                console.log('inserting downtime reasons');
                                await knex_agg.raw(`insert into tenant_plant_unit_machine_downtime_reasons (id,tenant_id,tenant_plant_unit_machine_downtime_id,reason_duration_in_min,reason_code,reason_description,created_at,updated_at) values(?,?,?,?,?,?,?,?)`,[inserting_data.id, tenantID, inserting_data.tenant_plant_unit_machine_downtime_id, inserting_data.reason_duration_in_min, inserting_data.reason_code,inserting_data.reason_description, inserting_data.created_at, inserting_data.updated_at]);
                            } else {
                                console.log("data is present", presentData[0].reason_duration_in_min)
                            }
                        }
                    }
                }
            }
        }

        async function lineDowntimes() {
            let lineId = await knex_line.raw(`select id from tenant_plant_unit_lines where tenant_id = ?`, [tenantID]);
            lineId = lineId.rows[0].id;
            let data = await knex_line.raw(`select * from tenant_plant_unit_line_downtimes where tenant_plant_unit_line_id = ? and start_time >= ? and start_time < ? order by start_time`,[lineId, fromDate.toDate(), toDate.toDate()]);
            data = data.rows;
            if (data.length) {
                for (let j = 0; j < data.length; j++) {
                    let inserting_data = data[j];
                    let presentData = await knex_agg.raw(`SELECT id,start_time from tenant_plant_unit_line_downtimes where id = ?`, [inserting_data.id]);
                    presentData = presentData.rows
                    if (!presentData.length) {
                        console.log('inserting line downtimes');
                        await knex_agg.raw(`INSERT INTO tenant_plant_unit_line_downtimes (id,tenant_id,tenant_plant_unit_line_id,type,start_time,end_time,work_order_id,created_at,updated_at,metadata) VALUES (?,?,?,?,?,?,?,?,?,?)`, [ inserting_data.id, tenantID, inserting_data.tenant_plant_unit_line_id, inserting_data.type, inserting_data.start_time, inserting_data.end_time, inserting_data.work_order_id, inserting_data.created_at, inserting_data.updated_at, JSON.stringify(inserting_data.metadata)]);
                    } else {
                        console.log("data is present", presentData[0].start_time)
                    }
                }
            }
        }

        async function statsData() {
             for (let i = 0; i < machineIds.length; i++) {
                let machineId = machineIds[i].id;
                let data = await knex_line.raw(`select * from tenant_plant_unit_machine_statistics where tenant_plant_unit_machine_id = ? and start_time >= ? and start_time < ? order by start_time`,[machineId, fromDate.toDate(), toDate.toDate()]);
                data = data.rows;
                if (data.length) {
                    for (let j = 0; j < data.length; j++) {
                        let inserting_data = data[j];
                        let presentData = await knex_agg.raw(`SELECT id,start_time from tenant_plant_unit_machine_statistics where id = ?`,[inserting_data.id]);
                        presentData = presentData.rows;
                        if(!presentData.length){
                            console.log('inserting stats data');
                            await knex_agg.raw(`INSERT INTO tenant_plant_unit_machine_statistics (id,tenant_id,tenant_plant_unit_machine_id,start_time,end_time,work_order_id,sku,operator,supervisor,standard_speed,wastage,total_production,planned_production,machine_stoppages,job_change,created_at,updated_at,average_speed,calcstat_parameters) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,[ inserting_data.id, tenantID, inserting_data.tenant_plant_unit_machine_id, inserting_data.start_time, inserting_data.end_time,inserting_data.work_order_id, inserting_data.sku, inserting_data.operator, inserting_data.supervisor, inserting_data.standard_speed, inserting_data.wastage, inserting_data.total_production, inserting_data.planned_production, inserting_data.machine_stoppages, inserting_data.job_change, inserting_data.created_at, inserting_data.updated_at, JSON.stringify(inserting_data.average_speed), JSON.stringify(inserting_data.calcstat_parameters)]);
                        } else {
                            console.log("data is present",presentData[0].start_time)
                        }
                    }
                }
            } 
        }

        async function opformData() {
            let keyspace = await knex_line.raw(`select sub_domain from tenants where id = ?`,[tenantID]);
            keyspace = keyspace.rows[0].sub_domain;
            console.log('line name ',keyspace);
            let opFormId = await knex_line.raw(`select id from tenant_operator_forms where name like ?`,[`%Wastage Form%`]);4
            opFormId = opFormId.rows[0].id;
            opFormId = opFormId.replace(/-/g,'_');
            let opformData = await timescale_line.raw(`SELECT * from ${keyspace}.opform_${opFormId} where inserted_at >= ? and inserted_at <= ?`,[fromDate, toDate]);
            opformData = opformData.rows;
            for (let j = 0; j < opformData.length; j++) {
                let inserting_data = opformData[j];
                let presentData = await timescale_agg.raw(`SELECT inserted_at from haldiram.opform_${opFormId} where inserted_at = ?`,[inserting_data.inserted_at]);
                presentData = presentData.rows;
                if(!presentData.length){
                    console.log('inserting opform data');
                    await timescale_agg.raw(`INSERT INTO haldiram.opform_${opFormId} (inserted_at,station_id,data_types,metadata,values) VALUES (?,?,?,?,?)`, [ inserting_data.inserted_at, inserting_data.station_id, inserting_data.data_types, inserting_data.metadata, JSON.stringify(inserting_data.values)]);
                }
                else {
                    console.log("data is present",presentData[0].inserted_at)
                }
            }   
        }

        await downtime();
        await downtimeReason();
        await lineDowntimes();
        await statsData();
        await opformData();
    }
 	mainFunc();