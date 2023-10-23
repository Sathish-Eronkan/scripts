let moment = require('moment');
const config = require('./config');
const knex_line = require('knex') ({
        client: 'pg',
        connection: {
                user: 'plantworks',
                host: lineIp,
                database: 'plantworks',
                password: 'plantworks',
                port: 5432,
        },
});
async function main() {
    let tenantID = config.tenantId;
    let subClass = config.subClass;
    let reasonCode = config.reasonCode;
    let unitName = await knex_line.knex.raw(`select name from tenant_plant_units where tenant_id = ?`, [tenantID]);
    unitName = unitName.rows[0].name;
    unitName = unitName.replace(/ /g,'_');
    let lineName = await knex_line.knex.raw(`select name from tenant_plant_unit_lines where tenant_id = ?`, [tenantID]);
    lineName = lineName.rows[0].name;
    lineName = lineName.replace(/ /g,'_');
    let reason_code = `${subClass}!${unitName.toUpperCase()}_${lineName.toUpperCase()}_${reasonCode}`;
    let reasonDesc = config.reasonDescription;
    let machineIds = await knex_line.knex.raw(`select id from tenant_plant_unit_machines where tenant_id = ?`,[tenantID]);
    machineIds = machineIds.rows;
    let currentTime = moment().tz('Asia/Kolkata').format();
    for(let i=0; i<machineIds.length; i++) {
        let machineId = machineIds[i].id;
        let downtimeData = await knex_line.knex.raw(`select id, start_time, end_time from tenant_plant_unit_machine_downtimes where tenant_plant_unit_machine_id = ? and metadata = ? order by start_time asc`,[machineId, '{}']);
        downtimeData = downtimeData.rows;
        if(downtimeData.length) {
            for(let j = 0; j < downtimeData.length; j++) {
                let downtime = downtimeData[j];
                const duration = moment(downtime['end_time'] || undefined).startOf('minute').diff(moment(downtime['start_time']).startOf('minute'), 'minutes');

                await knex_line.knex.raw(`update tenant_plant_unit_machine_downtimes set metadata = ? where id = ?`,['{"submited":true, "approved":true}',downtime.id]);

                await knex_line.knex.raw(`insert into tenant_plant_unit_machine_downtime_reasons (tenant_id, tenant_plant_unit_machine_downtime_id, reason_duration_in_min, reason_code, reason_description, created_at, updated_at) values(?,?,?,?,?,?,?)`,[tenantID, downtime.id, duration, reason_code, reasonDesc, currentTime, currentTime]);
            }
        }
    }
    console.log('Downtime Auto Submit is done');
}
console.log('Starting Downtime Auto Submit');
main();