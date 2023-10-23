let moment = require('moment');
const config = require('./config');
const lineIp = config.lineIp;
const aggIp = config.aggIp;
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

const knex_agg = require('knex') ({
        client: 'pg',
        connection: {
                user: 'plantworks',
                host: aggIp,
                database: 'plantworks',
                password: 'plantworks',
                port: 5432,
        },
});

async function main() {
    let tenantId = config.tenantId;
    let lineName = config.lineName;
    let emd_config = await knex_line.raw(`select id, name from tenant_emd_configurations where tenant_id = ?`,[tenantId]);
    emd_config = emd_config.rows;
    let masterdata_id = `%${lineName}%`;
    await knex_line.raw(`truncate tenant_emd_data`);
    for(let idx = 0; idx < emd_config.length; idx++) {    
        let emd_config_id = emd_config[idx].id;
        let emd_data_agg = await knex_agg.raw(`SELECT * FROM tenant_emd_data WHERE tenant_emd_configuration_id = ? and masterdata_id LIKE ? order by inserted_at`,[emd_config_id, masterdata_id]);
        emd_data_agg = emd_data_agg.rows
        
        for(let i = 0; i < emd_data_agg.length; i++ ){
            let emdData = emd_data_agg[i];
            await knex_line('tenant_emd_data').insert({
                    id: emdData.id,
                    tenant_id: emdData.tenant_id,
                    tenant_emd_configuration_id: emdData.tenant_emd_configuration_id,
                    masterdata_id: emdData.masterdata_id,
                    values: emdData.values,
                    data_types: emdData.data_types,
                    active: emdData.active,
                    inserted_at: emdData.inserted_at,
                    created_at: emdData.created_at,
                    rejected_at: emdData.rejected_at
            })
        }
    }
    console.log('Emd Data Sync is Done');
}


console.log('Starting Emd Data Data Sync')
main();