"use strict";

const AthenaExpress = require("athena-express"),
 aws = require("aws-sdk");

const AWS = require("aws-sdk");

/* 
 * AWS Credentials are not required here
 * because the IAM Role assumed by this Lambda
 * has the necessary permission to execute Athena queries
 * and store the result in Amazon S3 bucket
 */

const athenaExpressConfig = {
    aws,
    db: "level1_metadata",
    getStats: true
};

const dynamodb = new AWS.DynamoDB({apiVersion: "2012-08-10"});
const tableName = "tag";
const theme_tableName = "theme";

const athenaExpress = new AthenaExpress(athenaExpressConfig);

global.legal_scan_results = [];
global.legal_scan_result;
global.legal_scan_results_string;
global.legal_1;

global.emergency_scan_results = [];
global.emergency_scan_result;
global.emergency_scan_results_string;
global.emergency_1;

global.foundational_scan_results = [];
global.foundational_scan_result;
global.foundational_scan_results_string;

global.admin;
global.economy;
global.environment;
global.imagery;
global.infrastructure;
global.science;
global.society;
global.legal;
global.emergency;
global.foundational;

global.results;
global.foundational_search;
global.foundational_1;
global.foundational_only;


exports.handler = async(event, context, callback) => {
    console.log(event);
    
    var north;
    if (event.north === ''||event.north === ""||typeof event.north === 'undefined') {
        north = undefined;
    } else {
        north = event.north;
    }
    
    var east;
    if (event.east === ''||event.east === ""||typeof event.east === 'undefined') {
        east = undefined;
    } else {
        east = event.east;
    }
    
    var south;
    if (event.south === ''||event.south === ""||typeof event.south === 'undefined') {
        south = undefined;
    } else {
        south = event.south;
    }
    
    var west;
    if (event.west === ''||event.west === ""||typeof event.west === 'undefined') {
        west = undefined;
    } else {
        west = event.west;
    }
    var BB = "" + west + " " + south + ", " + east + " " + south + ", " + east + " " + north + ", " + west + " " + north + ", " + west + " " + south + "";
    
    
    var keyword;
    var keyword_lower;
    if (event.keyword === ''||event.keyword === ""||typeof event.keyword === 'undefined') {
        keyword = undefined;
    } else {
        keyword_lower = event.keyword;
        keyword = keyword_lower.toLowerCase();
    }
    
    var keyword_only;
    if (event.keyword_only === ''||event.keyword_only === ""||typeof event.keyword_only === 'undefined') {
        keyword_only = undefined;
    } else {
        keyword_only = event.keyword_only;
    }
    
    var min;
    if (event.min === ''||event.min === ""||typeof event.min === 'undefined') {
        min = "0";
    } else {
        min = event.min;
    }
    
    var max;
    if (event.max === ''||event.max === ""||typeof event.max === 'undefined') {
        max = "10";
    } else {
        max = event.max;
    }
    
    var lang;
    if (event.lang === ''||event.lang === ""||typeof event.lang === 'undefined') {
        lang = undefined;
    } else {
        lang = event.lang;
    }
    
    var theme;
    var theme_lower;
    if (event.theme === ''||event.theme === ""||typeof event.theme === 'undefined') {
        theme = undefined;
    } else {
        theme_lower = event.theme;
        theme = theme_lower.toLowerCase();
    }
    
    var org;
    var org_lower;
    if (event.org === ''||event.org === ""||typeof event.org === 'undefined') {
        org = undefined;
    } else {
        org_lower = event.org;
        org = org_lower.toLowerCase();
    }
    
    var types;
    var types_lower;
    if (event.type === ''||event.type === ""||typeof event.type === 'undefined') {
        types = undefined;
    } else {
        types_lower = event.type;
        types = types_lower.toLowerCase();
    }
    
    if (event.foundational === ''||event.foundational === ""||typeof event.foundational === 'undefined') {
        global.foundational_only = undefined;
    } else {
        global.foundational_only = event.foundational;
    }
    
    var begin_year;
    if (event.begin_year === ''||event.begin_year === ""||typeof event.begin_year === 'undefined') {
        begin_year = undefined;
    } else {
        begin_year = event.begin_year;
    }
    
    var end_year;
    if (event.end_year === ''||event.end_year === ""||typeof event.end_year === 'undefined') {
        end_year = undefined;
    } else {
        end_year = event.end_year;
    }
    
    var id = "COALESCE(features[1].properties.id, 'N/A') AS id";
    var coordinates = "features[1].geometry.coordinates AS coordinates";
    var publication_date = "COALESCE(features[1].properties.date.published.date, 'N/A') AS published";
    var options = "TRY(CAST(features[1].properties.options AS JSON)) AS options";
    var contact = "TRY(CAST(features[1].properties.contact AS JSON)) AS contact";
    var topicCategory = "COALESCE(features[1].properties.topicCategory, 'N/A') AS topicCategory";
    var created_date = "COALESCE(features[1].properties.date.created.date, 'N/A') AS created";
    var spatialRepresentation = "COALESCE(features[1].properties.spatialRepresentation, 'N/A') AS spatialRepresentation";
    var type = "COALESCE(features[1].properties.type, 'N/A') AS type";
    var temporalExtent = "TRY(features[1].properties.temporalExtent) AS temporalExtent";
    var graphicOverview = "CAST(features[1].properties.graphicOverview AS JSON) AS graphicOverview";
    var language = "COALESCE(features[1].properties.language, 'N/A') AS language";
    
    var time_begin = "SUBSTRING(COALESCE(features[1].properties.temporalExtent.begin, '0000'), 1, 4)";
    var time_end = "SUBSTRING(COALESCE(features[1].properties.temporalExtent.'end', '0000'), 1, 4)";
    //var time_search = 
    
    //(SELECT * FROM (SELECT * FROM metadata ORDER BY features[1].properties.id) WHERE " + time_search + " ORDER BY features[1].properties.id)

    //var refSys = "COALESCE(features[1].properties.refSys, 'N/A') AS refSys";
    //var refSys_version = "COALESCE(features[1].properties.refSys_version, 'N/A') AS refSys_version";
    //var status = "COALESCE(features[1].properties.status, 'N/A') AS status";
    //var maintenance = "COALESCE(features[1].properties.maintenance, 'N/A') AS maintenance";
    //var metadataStandard = "COALESCE(features[1].properties.metadataStandard.en, 'N/A') AS metadataStandard";
    //var metadataStandardVersion = "COALESCE(features[1].properties.metadataStandardVersion, 'N/A') AS metadataStandardVersion";
    //var distributionFormat_name = "COALESCE(features[1].properties.distributionFormat_name, 'N/A') AS distributionFormat_name";
    //var distributionFormat_format = "COALESCE(features[1].properties.distributionFormat_format, 'N/A') AS distributionFormat_format";
    //var useLimits = "COALESCE(features[1].properties.useLimits.en, 'N/A') AS useLimits";
    //var accessConstraints = "COALESCE(features[1].properties.accessConstraints, 'N/A') AS accessConstraints";
    //var otherConstraints = "COALESCE(features[1].properties.otherConstraints.en, 'N/A') AS otherConstraints";
    //var dateStamp = "COALESCE(features[1].properties.dateStamp, 'N/A') AS dateStamp";
    //var dataSetURI = "COALESCE(features[1].properties.dataSetURI, 'N/A') AS dataSetURI";
    //var locale = "TRY(features[1].properties.locale) AS locale";
    //var characterSet = "COALESCE(features[1].properties.characterSet, 'N/A') AS characterSet";
    //var environmentDescription = "COALESCE(features[1].properties.environmentDescription, 'N/A') as environmentDescription";
    //var supplementalInformation = "COALESCE(features[1].properties.supplementalInformation.en, 'N/A') AS supplementalInformation";
    //var credits = "TRY(CAST(features[1].properties.credits AS JSON)) AS credits";
    //var cited = "TRY(CAST(features[1].properties.cited AS JSON)) AS cited";
    //var distributor = "TRY(CAST(features[1].properties.distributor AS JSON)) AS distributor";
    
    var id_search = "regexp_like(features[1].properties.id, '" + keyword + "')";
    var topicCategory_search = "regexp_like(LOWER(features[1].properties.topicCategory), '" + keyword + "')";
    var type_search;
    
    let keywords;
    let title;
    let description;
    let keyword_search;
    let description_search;
    let title_search;
    let organisation;
    let organisation_search;
    let organisation_filter;
    
    if (lang === "fr") {
        
        keywords = "COALESCE(features[1].properties.keywords.fr, 'N/A') AS keywords";
        title = "COALESCE(features[1].properties.title.fr, 'N/A') AS title";
        description = "COALESCE(features[1].properties.description.fr, 'N/A') AS description";
        organisation = "COALESCE(CAST(json_extract(json_array_get(json_parse(features[1].properties.contact), 0), '$.organisation.fr') AS VARCHAR), 'N/A') AS organisation";
        
        keyword_search = "regexp_like(LOWER(features[1].properties.keywords.fr), '" + keyword + "')";
        description_search = "regexp_like(LOWER(features[1].properties.description.fr), '" + keyword + "')";
        title_search = "regexp_like(LOWER(features[1].properties.title.fr), '" + keyword + "')";
        organisation_search = "regexp_like(COALESCE(CAST(json_extract(json_array_get(json_parse(LOWER(features[1].properties.contact)), 0), '$.organisation.fr') AS VARCHAR), 'N/A'), '" + keyword + "')";
        organisation_filter = "regexp_like(COALESCE(CAST(json_extract(json_array_get(json_parse(LOWER(features[1].properties.contact)), 0), '$.organisation.fr') AS VARCHAR), 'N/A'), '" + org + "')";
        type_search = "(regexp_like(LOWER(features[1].properties.type), '" + types + "') OR regexp_like(COALESCE(CAST(json_extract(json_array_get(json_parse(LOWER(features[1].properties.options)), 0), '$.description.fr') AS VARCHAR), 'N/A'), '" + types + "'))";
        
    } else {
        
        keywords = "COALESCE(features[1].properties.keywords.en, 'N/A') AS keywords";
        title = "COALESCE(features[1].properties.title.en, 'N/A') AS title";
        description = "COALESCE(features[1].properties.description.en, 'N/A') AS description";
        organisation = "COALESCE(CAST(json_extract(json_array_get(json_parse(features[1].properties.contact), 0), '$.organisation.en') AS VARCHAR), 'N/A') AS organisation";
        
        keyword_search = "regexp_like(LOWER(features[1].properties.keywords.en), '" + keyword + "')";
        description_search = "regexp_like(LOWER(features[1].properties.description.en), '" + keyword + "')";
        title_search = "regexp_like(LOWER(features[1].properties.title.en), '" + keyword + "')";
        organisation_search = "regexp_like(COALESCE(CAST(json_extract(json_array_get(json_parse(LOWER(features[1].properties.contact)), 0), '$.organisation.en') AS VARCHAR), 'N/A'), '" + keyword + "')";
        organisation_filter = "regexp_like(COALESCE(CAST(json_extract(json_array_get(json_parse(LOWER(features[1].properties.contact)), 0), '$.organisation.en') AS VARCHAR), 'N/A'), '" + org + "')";
        type_search = "(regexp_like(LOWER(features[1].properties.type), '" + types + "') OR regexp_like(COALESCE(CAST(json_extract(json_array_get(json_parse(LOWER(features[1].properties.options)), 0), '$.description.en') AS VARCHAR), 'N/A'), '" + types + "'))";
        
    }
    
    var search = "" + id_search + " OR " + topicCategory_search + " OR " + keyword_search + " OR " + description_search + " OR " + title_search + " OR " + organisation_search + "";

    var display_fields = "" + id + ", " + coordinates + ", " + title + ", " + description + ", " + publication_date + ", " + keywords + ", " + options + ", " + contact + ", " + topicCategory + ", " + created_date + ", " + spatialRepresentation + ", " + type + ", " + temporalExtent + ", " + graphicOverview + ", " + language + ", " + organisation + "";
    //, " + supplementalInformation + ", " + credits + ", " + distributor + "";

    //var display_fields = "" + id + ", " + coordinates + ", " + title + ", " + description + ", " + publication_date + ", " + keywords + ", " + options + ", " + contact + ", " + topicCategory + ", " + created_date + ", " + spatialRepresentation + ", " + type + ", " + temporalExtent + ", " + refSys + ", " + refSys_version + ", " + status + ", " + maintenance + ", " + metadataStandard + ", " + metadataStandardVersion + ", " + graphicOverview + ", " + distributionFormat_name + ", " + distributionFormat_format + ", " + useLimits + ", " + accessConstraints + ", " + otherConstraints + ", " + dateStamp + ", " + dataSetURI + ", " + locale + ", " + language + ", " + characterSet + ", " + environmentDescription + ", " + supplementalInformation + ", " + credits + ", " + distributor + "";
    
    var theme_search = '';
    
function themes() {
    
    return new Promise(resolve => {

if (typeof theme !== 'undefined') {
    
    if (theme.includes('administration')) {
        global.admin = "boundaries|planning_cadastre|location";
    } else {
        global.admin = "";
    }
    
    if (theme.includes('economy')) {
        global.economy = "economy|farming";
    } else {
        global.economy = "";
    }
    
    if (theme.includes('environment')) {
        global.environment = "biota|environment|elevation|inland_waters|oceans|climatologyMeteorologyAtmosphere";
    } else {
        global.environment = "";
    }
    
    if (theme.includes('imagery')) {
        global.imagery = "imageryBaseMapsEarthCover";
    } else {
        global.imagery = "";
    }
    
    if (theme.includes('infrastructure')) {
        global.infrastructure = "structure|transport|utilitiesCommunication";
    } else {
        global.infrastructure = "";
    }
    
    if (theme.includes('science')) {
        global.science = "geoscientificInformation";
    } else {
        global.science = "";
    }
    
    if (theme.includes('society')) {
        global.society = "health|society|intelligenceMilitary";
    } else {
        global.society = "";
    }
    
    var pre_theme_string = global.admin + global.economy + global.environment + global.imagery + global.infrastructure + global.science + global.society;
    var pre_theme_string_trim = pre_theme_string.slice(0, -1);
    
    if (theme.includes('legal')) {
    
        var params = {
            TableName: theme_tableName,
            FilterExpression: "tag = :tag",
            ExpressionAttributeValues: {
                ":tag":{"S":"legal"},
        }};
        
        dynamodb.scan(params, function(err, data) {
        
        if (err) {
            console.log("Error", err);
        } else {
            
            data.Items.forEach(function(result, index, array) {
                
                global.legal_scan_result = result.uuid.S + "|";
                global.legal_scan_results.push(global.legal_scan_result); 
                
            });
            
        }
        
        global.legal_scan_results_string = String(global.legal_scan_results);
        global.legal_1 = global.legal_scan_results_string.replace(/\,/g, '');
        global.legal = global.legal_1.slice(0, -1);
        
        });

    }
    
    if (theme.includes('emergency')) {
        
        params = {
        TableName: theme_tableName,
        FilterExpression: "tag = :tag",
        ExpressionAttributeValues: {
            ":tag":{"S":"emergency"},
        }};
        
        dynamodb.scan(params, function(err, data) {
        
        if (err) {
            console.log("Error", err);
        } else {
            
            data.Items.forEach(function(result, index, array) {
                
                global.emergency_scan_result = result.uuid.S + "|";
                global.emergency_scan_results.push(global.emergency_scan_result); 
                
            });
            
        }
        
        global.emergency_scan_results_string = String(global.emergency_scan_results);
        global.emergency_1 = global.emergency_scan_results_string.replace(/\,/g, '');
        global.emergency = global.emergency_1.slice(0, -1);
        
        });
        
    }
    
    var pre_tag_theme_string = global.legal + global.emergency;
    var pre_tag_theme_string_trim = String(pre_tag_theme_string).slice(0, -1);
    
    
    if ( theme.includes('administration')||theme.includes('economy')||theme.includes('environment')||theme.includes('imagery')||theme.includes('infrastructure')||theme.includes('science')||theme.includes('society') ) {
        
        if (!theme.includes('legal')||!theme.includes('emergency')) {
            
            theme_search = "regexp_like(features[1].properties.topicCategory, '" + pre_theme_string_trim + "')";
            
        } else {
            
            theme_search = "regexp_like(features[1].properties.topicCategory, '" + pre_theme_string_trim + "') AND regexp_like(features[1].properties.id, '" + pre_tag_theme_string_trim + "')";
             
        }
    
    } 
    
    if (theme.includes('legal')||theme.includes('emergency')) { 
        
        if (!theme.includes('administration')||!theme.includes('economy')||!theme.includes('environment')||!theme.includes('imagery')||!theme.includes('infrastructure')||!theme.includes('science')||!theme.includes('society') ) {
        
            theme_search = "regexp_like(features[1].properties.id, '" + pre_tag_theme_string_trim + "')";
    
        } else {
            
            theme_search = "regexp_like(features[1].properties.topicCategory, '" + pre_theme_string_trim + "') AND regexp_like(features[1].properties.id, '" + pre_tag_theme_string_trim + "')";
        }
    }
    
    
}
        
    resolve();
    });
}

function foundational() {

return new Promise(resolve => {

    if (global.foundational_only === "true") {
    
        var params = {
            TableName: "foundational",
            FilterExpression: "loc = :loc",
            ExpressionAttributeValues: {
                ":loc":{"S":"foundational"},
        }};
        
        dynamodb.scan(params, function(err, data) {
        
        if (err) {
            console.log("Error", err);
        } else {
            
            data.Items.forEach(function(result, index, array) {
                
                global.foundational_scan_result = result.uuid.S + "|";
                global.foundational_scan_results.push(global.foundational_scan_result); 
                
            });

            global.foundational_scan_results_string = String(global.foundational_scan_results);
            global.foundational_1 = global.foundational_scan_results_string.replace(/\,/g, '');
            global.foundational = global.foundational_1.slice(0, -1);
            
            
        }
        
        });

    }

 resolve();   
});
 
}

function time() {
    return new Promise(resolve => {
    setTimeout(() => {
        
        global.foundational_search = "regexp_like(features[1].properties.id, '" + global.foundational + "')";
        
        //console.log(global.foundational_search);
    
    resolve();
    }, 100);
});
} 

 function query() {
      
        return new Promise(async function(resolve) {  
            
            let sqlQuery;
            
        if (typeof global.foundational_only === 'undefined') {
    
        if (typeof types === 'undefined') {

            if (typeof org === 'undefined') {
    
                if (typeof theme === 'undefined') {

                    if (keyword_only === "true") {
            
                        if (typeof keyword === 'undefined') {
        
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM metadata) AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM metadata ORDER BY features[1].properties.id ASC)) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
    
                        } else {
                
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM metadata WHERE (" + search + ")) AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM metadata ORDER BY features[1].properties.id ASC) WHERE (" + search + ") ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
                
                        }
                
                
                    } else {
    
                        if (typeof keyword === 'undefined') {
    
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM metadata WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))'))) AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM (SELECT * FROM metadata ORDER BY features[1].properties.id) WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))'))) ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
               
                        } else {
        
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM (SELECT * FROM metadata WHERE (" + search + ") ) WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))'))) AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM metadata ORDER BY features[1].properties.id) WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))')) ORDER BY features[1].properties.id ASC) WHERE (" + search + ") ORDER BY features[1].properties.id ASC)  ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
               
                        }
                    }
        
                } else {
     
                    if (keyword_only === "true") {
            
                        if (typeof keyword === 'undefined') {
        
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM metadata WHERE " + theme_search + ") AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM metadata ORDER BY features[1].properties.id ASC) WHERE " + theme_search + " ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
    
                        } else {
                
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM metadata WHERE (" + search + ") AND " + theme_search + " ) AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM metadata ORDER BY features[1].properties.id ASC) WHERE (" + search + ") AND " + theme_search + " ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
                
                        }
                
                    } else {
    
                        if (typeof keyword === 'undefined') {
    
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM (SELECT * FROM metadata WHERE " + theme_search + " ) WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))'))) AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM (SELECT * FROM metadata ORDER BY features[1].properties.id) WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))')) ORDER BY features[1].properties.id ASC)  ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
    
                        } else {
        
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM (SELECT * FROM metadata WHERE (" + search + ") AND " + theme_search + " ) WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))'))) AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM metadata ORDER BY features[1].properties.id) WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))')) ORDER BY features[1].properties.id ASC) WHERE (" + search + ") AND " + theme_search + " ORDER BY features[1].properties.id ASC)  ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
    
                        }
                    }
    
                }
    
            } else {
                
                if (typeof theme === 'undefined') {

                    if (keyword_only === "true") {
            
                        if (typeof keyword === 'undefined') {
        
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM metadata WHERE " + organisation_filter + ") AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM metadata ORDER BY features[1].properties.id ASC) WHERE " + organisation_filter + " ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
    
                        } else {
                
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM metadata WHERE (" + search + ") AND " + organisation_filter + ") AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM metadata ORDER BY features[1].properties.id ASC) WHERE (" + search + ") AND " + organisation_filter + " ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
                
                        }
            
                    } else {
    
                        if (typeof keyword === 'undefined') {
    
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM (SELECT * FROM metadata WHERE " + organisation_filter + " ) WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))'))) AS total FROM (SELECT *, row_number() over () AS row_num, features[1].properties.id AS ID, ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))') FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM metadata ORDER BY features[1].properties.id) WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))')) ORDER BY features[1].properties.id ASC) WHERE " + organisation_filter + " ORDER BY features[1].properties.id ASC)  ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
               
                        } else {
        
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM (SELECT * FROM metadata WHERE (" + search + ") AND " + organisation_filter + ") WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))'))) AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM metadata ORDER BY features[1].properties.id)  WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))')) ORDER BY features[1].properties.id ASC) WHERE (" + search + ") AND " + organisation_filter + " ORDER BY features[1].properties.id ASC) ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
               
                        }
                    }
        
                } else {
     
                    if (keyword_only === "true") {
            
                        if (typeof keyword === 'undefined') {
        
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM metadata WHERE (" + theme_search + ") AND " + organisation_filter + ") AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM metadata ORDER BY features[1].properties.id ASC) WHERE (" + theme_search + ") AND " + organisation_filter + " ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
    
                        } else {
                            
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM metadata WHERE (" + search + ") AND (" + theme_search + ") AND (" + organisation_filter + ")) AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM metadata ORDER BY features[1].properties.id ASC) WHERE (" + search + ") AND (" + theme_search + ") AND " + organisation_filter + " ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
                
                        }
                
                    } else {
    
                        if (typeof keyword === 'undefined') {
    
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM (SELECT * FROM metadata WHERE (" + theme_search + ") AND " + organisation_filter + ") WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))'))) AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM metadata ORDER BY features[1].properties.id) WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))')) ORDER BY features[1].properties.id ASC) WHERE (" + theme_search + ") AND " + organisation_filter + " ORDER BY features[1].properties.id ASC)  ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
    
                        } else {
                            
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM (SELECT * FROM metadata WHERE (" + search + ") AND (" + theme_search + ") AND " + organisation_filter + " ) WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))'))) AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM (SELECT * FROM metadata ORDER BY features[1].properties.id) WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))')) ORDER BY features[1].properties.id ASC) WHERE (" + search + ") AND (" + theme_search + ") AND " + organisation_filter + " ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
    
                        }
                    }
    
                }

            }
    
        } else {
        
            if (typeof org === 'undefined') {
    
                if (typeof theme === 'undefined') {

                    if (keyword_only === "true") {
            
                        if (typeof keyword === 'undefined') {
        
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM metadata WHERE " + type_search + ") AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM metadata ORDER BY features[1].properties.id ASC) WHERE " + type_search + " ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
    
                        } else {
                
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM metadata WHERE (" + search + ") AND " + type_search + ") AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM metadata ORDER BY features[1].properties.id ASC) WHERE (" + search + ") AND " + type_search + " ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
                
                        }
                
                
                    } else {
    
                        if (typeof keyword === 'undefined') {
    
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM (SELECT * FROM metadata WHERE " + type_search + ") WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))'))) AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM metadata ORDER BY features[1].properties.id) WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))')) ORDER BY features[1].properties.id ASC) WHERE " + type_search + " ORDER BY features[1].properties.id ASC) ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
               
                        } else {
        
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM (SELECT * FROM metadata WHERE (" + search + ") AND " + type_search + ") WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))'))) AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM metadata ORDER BY features[1].properties.id) WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))')) ORDER BY features[1].properties.id ASC) WHERE (" + search + ") AND " + type_search + " ORDER BY features[1].properties.id ASC) ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
               
                        }
                    }
        
                } else {
     
                    if (keyword_only === "true") {
            
                        if (typeof keyword === 'undefined') {
        
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM metadata WHERE (" + theme_search + ") AND " + type_search + ") AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM metadata ORDER BY features[1].properties.id ASC) WHERE (" + theme_search + ") AND " + type_search + " ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
    
                        } else {
                
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM metadata WHERE (" + search + ") AND (" + theme_search + ") AND " + type_search + ") AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM metadata ORDER BY features[1].properties.id ASC) WHERE (" + search + ") AND (" + theme_search + ") AND " + type_search + ") ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
                
                        }
                
                    } else {
    
                        if (typeof keyword === 'undefined') {
    
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM (SELECT * FROM metadata WHERE (" + theme_search + ") AND " + type_search + ") WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))'))) AS total FROM (SELECT *, row_number() over () AS row_num, features[1].properties.id AS ID, ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))') FROM (SELECT * FROM (SELECT * FROM metadata ORDER BY features[1].properties.id ASC) WHERE (" + theme_search + ") AND " + type_search + " ORDER BY features[1].properties.id ASC) WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))')) ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
    
                        } else {
        
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM (SELECT * FROM metadata WHERE (" + search + ") AND (" + theme_search + ") AND " + type_search + ") WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))'))) AS total FROM (SELECT *, row_number() over () AS row_num, features[1].properties.id AS ID, ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))') FROM (SELECT * FROM (SELECT * FROM metadata ORDER BY features[1].properties.id ASC) WHERE (" + search + ") AND " + theme_search + " AND " + type_search + " ORDER BY features[1].properties.id ASC) WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))')) ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
    
                        }
                    }
    
                }
    
            } else {
        
                if (typeof theme === 'undefined') {

                    if (keyword_only === "true") {
            
                        if (typeof keyword === 'undefined') {
        
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM metadata WHERE " + organisation_filter + " AND " + type_search + ") AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM metadata ORDER BY features[1].properties.id ASC) WHERE " + organisation_filter + " AND " + type_search + " ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
    
                        } else {
                
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM metadata WHERE (" + search + ") AND " + organisation_filter + " AND " + type_search + ") AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM metadata ORDER BY features[1].properties.id ASC) WHERE (" + search + ") AND " + organisation_filter + " AND " + type_search + " ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
                
                        }
            
                    } else {
    
                        if (typeof keyword === 'undefined') {
                            
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM (SELECT * FROM metadata WHERE " + organisation_filter + " AND " + type_search + " ) WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))')) AND " + organisation_filter + " AND " + type_search + ") AS total FROM (SELECT * FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM metadata WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))')) ORDER BY features[1].properties.id ASC) ORDER BY features[1].properties.id ASC) WHERE (" + search + ") AND " + organisation_filter + " AND " + type_search + " ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
               
                        } else {
        
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM (SELECT * FROM metadata WHERE (" + search + ") AND " + organisation_filter + " AND " + type_search + ") WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))'))) AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM (SELECT * FROM metadata WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))')) ORDER BY features[1].properties.id ASC) WHERE (" + search + ") AND " + organisation_filter + " AND " + type_search + " ORDER BY features[1].properties.id ASC)  ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
               
                        }
                    }
        
                } else {
     
                    if (keyword_only === "true") {
            
                        if (typeof keyword === 'undefined') {
        
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM metadata WHERE " + organisation_filter + " AND " + theme_search + " AND " + type_search + ") AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM metadata ORDER BY features[1].properties.id ASC) WHERE " + organisation_filter + " AND " + theme_search + " AND " + type_search + " ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
    
                        } else {
                
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM metadata WHERE (" + search + ") AND " + organisation_filter + " AND " + theme_search + " AND " + type_search + " ) AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM metadata ORDER BY features[1].properties.id ASC) WHERE (" + search + ") AND " + organisation_filter + " AND " + theme_search + " AND " + type_search + " ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
                
                        }
                
                    } else {
    
                        if (typeof keyword === 'undefined') {
    
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM (SELECT * FROM metadata WHERE " + organisation_filter + " AND " + theme_search + " AND " + type_search + ") WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))'))) AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM (SELECT * FROM metadata WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))')) ORDER BY features[1].properties.id ASC) WHERE " + organisation_filter + " AND " + theme_search + " AND " + type_search + " ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
    
                        } else {
        
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM (SELECT * FROM metadata WHERE (" + search + ") AND " + organisation_filter + " AND " + theme_search + " AND " + type_search + ") WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))'))) AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM (SELECT * FROM metadata WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))')) ORDER BY features[1].properties.id ASC) WHERE (" + search + ") AND " + organisation_filter + " AND " + theme_search + " AND " + type_search + " ORDER BY features[1].properties.id ASC) ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
    
                        }
                    }
    
                }

            }
    
        }
        
            } else {
        
        if (typeof types === 'undefined') {

            if (typeof org === 'undefined') {
    
                if (typeof theme === 'undefined') {

                    if (keyword_only === "true") {
            
                        if (typeof keyword === 'undefined') {
        
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM metadata WHERE " + global.foundational_search + ") AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM metadata ORDER BY features[1].properties.id ASC) WHERE " + global.foundational_search + " ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
    
                        } else {
                
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM metadata WHERE (" + search + ") AND " + global.foundational_search + ") AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM metadata ORDER BY features[1].properties.id ASC) WHERE (" + search + ") AND " + global.foundational_search + " ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
                
                        }
                
                
                    } else {
    
                        if (typeof keyword === 'undefined') {
    
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM (SELECT * FROM metadata WHERE " + global.foundational_search + ") WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))'))) AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM metadata ORDER BY features[1].properties.id) WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))')) ORDER BY features[1].properties.id ASC) WHERE " + global.foundational_search + " ORDER BY features[1].properties.id ASC)  ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
               
                        } else {
        
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM (SELECT * FROM metadata WHERE (" + search + ") AND " + global.foundational_search + ") WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))'))) AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM metadata ORDER BY features[1].properties.id) WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))')) ORDER BY features[1].properties.id ASC) WHERE (" + search + ") AND " + global.foundational_search + " ORDER BY features[1].properties.id ASC)  ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
               
                        }
                    }
        
                } else {
                    

                    if (keyword_only === "true") {
            
                        if (typeof keyword === 'undefined') {
        
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM metadata WHERE " + theme_search + " AND " + global.foundational_search + ") AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM metadata ORDER BY features[1].properties.id ASC) WHERE " + theme_search + " AND " + global.foundational_search + " ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
    
                        } else {
                
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM metadata WHERE (" + search + ") AND " + theme_search + " AND " + global.foundational_search + ") AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM metadata ORDER BY features[1].properties.id ASC) WHERE (" + search + ") AND " + theme_search + " AND " + global.foundational_search + " ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
                
                        }
                
                    } else {
                        
                        if (typeof keyword === 'undefined') {
    
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM (SELECT * FROM metadata WHERE " + theme_search + " AND " + global.foundational_search + " ) WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))'))) AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM (SELECT * FROM metadata WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))')) ORDER BY features[1].properties.id ASC) WHERE " + theme_search + " AND " + global.foundational_search + " ORDER BY features[1].properties.id ASC)  ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
    
                        } else {
        
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM (SELECT * FROM metadata WHERE (" + search + ") AND " + theme_search + " AND " + global.foundational_search + " ) WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))'))) AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM (SELECT * FROM metadata WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))')) ORDER BY features[1].properties.id ASC) WHERE (" + search + ") AND " + theme_search + " AND " + global.foundational_search + " ORDER BY features[1].properties.id ASC) ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
    
                        }
                    }
    
                }
    
            } else {
                
                if (typeof theme === 'undefined') {

                    if (keyword_only === "true") {
            
                        if (typeof keyword === 'undefined') {
        
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM metadata WHERE " + organisation_filter + " AND " + global.foundational_search + ") AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM metadata ORDER BY features[1].properties.id ASC) WHERE " + organisation_filter + " AND " + global.foundational_search + " ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
    
                        } else {
                
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM metadata WHERE (" + search + ") AND " + organisation_filter + " AND " + global.foundational_search + ") AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM metadata ORDER BY features[1].properties.id ASC) WHERE (" + search + ") AND " + organisation_filter + " AND " + global.foundational_search + " ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
                
                        }
            
                    } else {
    
                        if (typeof keyword === 'undefined') {
    
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM (SELECT * FROM metadata WHERE " + organisation_filter + " AND " + global.foundational_search + ") WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))'))) AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM metadata ORDER BY features[1].properties.id) WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))')) ORDER BY features[1].properties.id ASC) WHERE " + organisation_filter + " AND " + global.foundational_search + " ORDER BY features[1].properties.id ASC) ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
               
                        } else {
        
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM (SELECT * FROM metadata WHERE (" + search + ") AND " + organisation_filter + " AND " + global.foundational_search + ") WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))'))) AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM metadata ORDER BY features[1].properties.id) WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))')) ORDER BY features[1].properties.id ASC) WHERE (" + search + ") AND " + organisation_filter + " AND " + global.foundational_search + " ORDER BY features[1].properties.id ASC) ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
               
                        }
                    }
        
                } else {
     
                    if (keyword_only === "true") {
            
                        if (typeof keyword === 'undefined') {
        
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM metadata WHERE " + theme_search + " AND " + organisation_filter + " AND " + global.foundational_search + ") AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM metadata ORDER BY features[1].properties.id ASC) WHERE " + theme_search + " AND " + organisation_filter + " AND " + global.foundational_search + " ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
    
                        } else {
                
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM metadata WHERE (" + search + ") AND " + theme_search + " AND " + organisation_filter + " AND " + global.foundational_search + " ) AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM metadata ORDER BY features[1].properties.id ASC) WHERE (" + search + ") AND " + theme_search + " AND " + organisation_filter + " AND " + global.foundational_search + " ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
                
                        }
                
                    } else {
    
                        if (typeof keyword === 'undefined') {
    
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM (SELECT * FROM metadata WHERE " + theme_search + " AND " + organisation_filter + " AND " + global.foundational_search + " ) WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))'))) AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM (SELECT * FROM metadata WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))')) ORDER BY features[1].properties.id ASC) WHERE " + theme_search + " AND " + organisation_filter + " AND " + global.foundational_search + " ORDER BY features[1].properties.id ASC) ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
    
                        } else {
        
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM (SELECT * FROM metadata WHERE (" + search + ") AND " + theme_search + " AND " + organisation_filter + " AND " + global.foundational_search + ") WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))'))) AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM (SELECT * FROM metadata WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))')) ORDER BY features[1].properties.id ASC) WHERE (" + search + ") AND " + theme_search + " AND " + organisation_filter + " AND " + global.foundational_search + " ORDER BY features[1].properties.id ASC)  ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
    
                        }
                    }
    
                }

            }
    
        } else {
        
            if (typeof org === 'undefined') {
    
                if (typeof theme === 'undefined') {

                    if (keyword_only === "true") {
            
                        if (typeof keyword === 'undefined') {
        
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM metadata WHERE " + global.foundational_search + ") AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM metadata ORDER BY features[1].properties.id ASC) WHERE " + global.foundational_search + " ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
    
                        } else {
                
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM metadata WHERE (" + search + ") AND " + global.foundational_search + ") AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM metadata ORDER BY features[1].properties.id ASC) WHERE (" + search + ") AND " + global.foundational_search + " ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
                
                        }
                
                
                    } else {
    
                        if (typeof keyword === 'undefined') {
    
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM (SELECT * FROM metadata WHERE " + global.foundational_search + ") WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))'))) AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM (SELECT * FROM metadata WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))')) ORDER BY features[1].properties.id ASC) WHERE " + global.foundational_search + " ORDER BY features[1].properties.id ASC) ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
               
                        } else {
        
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM (SELECT * FROM metadata WHERE (" + search + ") AND " + global.foundational_search + ") WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))'))) AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM (SELECT * FROM metadata WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))')) ORDER BY features[1].properties.id ASC) WHERE (" + search + ") AND " + global.foundational_search + " ORDER BY features[1].properties.id ASC) ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
               
                        }
                    }
        
                } else {
     
                    if (keyword_only === "true") {
            
                        if (typeof keyword === 'undefined') {
        
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM metadata WHERE " + theme_search + " AND " + type_search + " AND " + global.foundational_search + ") AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM metadata ORDER BY features[1].properties.id ASC) WHERE " + theme_search + " AND " + type_search + " AND " + global.foundational_search + " ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
    
                        } else {
                
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM metadata WHERE (" + search + ") AND " + theme_search + " AND " + type_search + " AND " + global.foundational_search + ") AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM metadata ORDER BY features[1].properties.id ASC) WHERE (" + search + ") AND " + theme_search + " AND " + type_search + " AND " + global.foundational_search + " ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
                
                        }
                
                    } else {
    
                        if (typeof keyword === 'undefined') {
    
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM (SELECT * FROM metadata WHERE " + theme_search + " AND " + type_search + " AND " + global.foundational_search + ") WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))'))) AS total FROM (SELECT *, row_number() over () AS row_num, features[1].properties.id AS ID, ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))') FROM (SELECT * FROM (SELECT * FROM metadata ORDER BY features[1].properties.id ASC) WHERE " + theme_search + " AND " + type_search + " AND " + global.foundational_search + " ORDER BY features[1].properties.id ASC) WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))')) ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
    
                        } else {
        
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM (SELECT * FROM metadata WHERE (" + search + ") AND " + theme_search + " AND " + type_search + " AND " + global.foundational_search + " ) WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))'))) AS total FROM (SELECT *, row_number() over () AS row_num, features[1].properties.id AS ID, ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))') FROM (SELECT * FROM (SELECT * FROM metadata ORDER BY features[1].properties.id ASC) WHERE (" + search + ") AND " + theme_search + " AND " + type_search + " AND " + global.foundational_search + " ORDER BY features[1].properties.id ASC) WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))')) ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
    
                        }
                    }
    
                }
    
            } else {
                
                if (typeof theme === 'undefined') {

                    if (keyword_only === "true") {
            
                        if (typeof keyword === 'undefined') {
        
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM metadata WHERE " + organisation_filter + " AND " + type_search + " AND " + global.foundational_search + ") AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM metadata ORDER BY features[1].properties.id ASC) WHERE " + organisation_filter + " AND " + type_search + " AND " + global.foundational_search + " ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
    

                        } else {
                
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM metadata WHERE (" + search + ") AND " + organisation_filter + " AND " + type_search + " AND " + global.foundational_search + ") AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM metadata ORDER BY features[1].properties.id ASC) WHERE (" + search + ") AND " + type_search + " AND " + organisation_filter + " AND " + global.foundational_search + " ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
                
                        }
            
                    } else {
    
                        if (typeof keyword === 'undefined') {
    
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM (SELECT * FROM metadata WHERE " + organisation_filter + " AND " + type_search + " AND " + global.foundational_search + " ) WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))'))) AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM (SELECT * FROM metadata  WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))')) ORDER BY features[1].properties.id ASC) WHERE " + organisation_filter + " AND " + type_search + " AND " + global.foundational_search + " ORDER BY features[1].properties.id ASC) ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
               
                        } else {
        
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM (SELECT * FROM metadata WHERE (" + search + ") AND " + organisation_filter + " AND " + type_search + " AND " + global.foundational_search + " ) WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))'))) AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM (SELECT * FROM metadata WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))')) ORDER BY features[1].properties.id ASC) WHERE (" + search + ") AND " + organisation_filter + " AND " + type_search + " AND " + global.foundational_search + " ORDER BY features[1].properties.id ASC)  ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
               
                        }
                    }
        
                } else {
     
                    if (keyword_only === "true") {
            
                        if (typeof keyword === 'undefined') {
        
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM metadata WHERE " + organisation_filter + " AND " + theme_search + " AND " + type_search + " AND " + global.foundational_search + ") AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM metadata ORDER BY features[1].properties.id ASC) WHERE " + organisation_filter + " AND " + theme_search + " AND " + type_search + " AND " + global.foundational_search + " ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
    
                        } else {
                
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM metadata WHERE (" + search + ") AND " + organisation_filter + " AND " + theme_search + " AND " + type_search + " AND " + global.foundational_search + ") AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM metadata ORDER BY features[1].properties.id ASC) WHERE (" + search + ") AND " + organisation_filter + " AND " + theme_search + " AND " + type_search + " AND " + global.foundational_search + " ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
                
                        }
                
                    } else {
    
                        if (typeof keyword === 'undefined') {
    
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM (SELECT * FROM metadata WHERE " + organisation_filter + " AND " + theme_search + " AND " + type_search + " AND " + global.foundational_search + ") WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))'))) AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM (SELECT * FROM metadata  WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))')) ORDER BY features[1].properties.id ASC) WHERE (" + search + ") AND " + theme_search + " AND " + type_search + " AND " + organisation_filter + " AND " + global.foundational_search + " ORDER BY features[1].properties.id ASC) ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
    
                        } else {
        
                            sqlQuery = "SELECT row_num, " + display_fields + ", (SELECT COUNT(features[1].properties.id) FROM (SELECT * FROM metadata WHERE (" + search + ") AND " + organisation_filter + " AND " + theme_search + " AND " + type_search + " AND " + global.foundational_search + ") WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))'))) AS total FROM (SELECT *, row_number() over () AS row_num FROM (SELECT * FROM (SELECT * FROM metadata WHERE ST_INTERSECTS( ST_POLYGON(features[1].properties.geometry), ST_POLYGON('POLYGON((" + BB + "))')) ORDER BY features[1].properties.id ASC) WHERE (" + search + ") AND " + theme_search + " AND " + type_search + " AND " + organisation_filter + " AND " + global.foundational_search + " ORDER BY features[1].properties.id ASC) ORDER BY features[1].properties.id ASC) WHERE row_num BETWEEN " + min + " AND " + max + " ORDER BY features[1].properties.id ASC";
    
                        }
                    }
    
                }

            }
    
        }
    
    }
    
            //console.log(global.foundational_search);
            console.log(sqlQuery);
            
            global.results = await athenaExpress.query(sqlQuery);
            
            resolve();
        
        });
        
    }    

    
    await themes();
    await foundational();
    await time();
    await query();
        
    //console.log(results);
    
    callback(null, global.results);
};
