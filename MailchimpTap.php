<?php

use GuzzleHttp\Client;
use SingerPhp\SingerTap;
use SingerPhp\Singer;

class MailchimpTap extends SingerTap
{
    public function __construct()
    {
        parent::__construct();
    }

    /**
     * Data Center. Ex: us14
     */
    private $dataCenter = '';

    /**
     * API Key
     */
    private $token = '';

    /**
     * Current Table being processed.
     */
    private $table = '';

    /**
     * Maximum number of retries per request
     */
    private int $max_retries = 10;

    /**
     * Number of seconds to wait between retries
     */
    private int $retry_delay = 15;

    /**
     * Number of seconds to wait when we hit a rate limit response
     */
    private int $rate_limit_delay = 90;

    /**
     * Tests if the connector is working then writes the results to STDOUT
     */
    public function test()
    {
        $this->token        = $this->singer->config->input('token');
        $this->dataCenter   = $this->singer->config->input('dataCenter');

        try {
            $response = $this->request(
                "GET",
                "/lists"
            );

            $this->singer->writeMeta(['test_result' => true]);
        } catch (Exception $e) {
            $this->singer->writeMeta(['test_result' => false]);
        }
    }

    /**
     * Gets all schemas/tables and writes the results to STDOUT
     */
    public function discover()
    {
        $this->singer->logger->debug('Starting discover for tap Mailchimp');

        $this->token        = $this->singer->config->setting('token');
        $this->dataCenter   = $this->singer->config->setting('dataCenter');

        foreach ($this->singer->config->catalog->streams as $stream) {
            $this->table = strtolower($stream->stream);

            $column_map = $this->table_map[$this->table]['properties'];
            $indexes = $this->table_map[$this->table]['indexes'];

            if (! array_key_exists('use_unique_keys', $this->table_map[$this->table]) || $this->table_map[$this->table]['use_unique_keys'] === TRUE) {
                $this->singer->writeMeta(['unique_keys' => $indexes]);
            }

            $columns = [];
            foreach($column_map as $colName => $colType) {
                $types = [
                    'boolean' => Singer::TYPE_BOOLEAN,
                    'int' => Singer::TYPE_INTEGER,
                    'object'  => Singer::TYPE_OBJECT,
                    'string'  => Singer::TYPE_STRING,
                    'timestampz'  => Singer::TYPE_TIMESTAMPTZ,
                ];
                $type = array_key_exists($colType, $types) ? $types[$colType] : Singer::TYPE_STRING;

                $columns[$colName] = [
                    'type' => $type
                ];
            }

            $this->singer->logger->debug('columns', [$columns]);
            $this->singer->logger->debug('indexes', [$indexes]);

            $this->singer->writeSchema(
                stream: $stream->stream,
                schema: $columns,
                key_properties: $indexes
            );
        }
    }

    /**
     * Gets the record data and writes to STDOUT
     */
    public function tap()
    {
        $this->singer->logger->debug('Starting sync for tap Mailchimp');
        $this->singer->logger->debug("catalog", [$this->singer->config->catalog]);

        $this->token        = $this->singer->config->setting('token');
        $this->dataCenter   = $this->singer->config->setting('dataCenter');

        foreach ($this->singer->config->catalog->streams as $stream) {
            $this->table = strtolower($stream->stream);
            $this->singer->logger->debug("Starting sync for {$this->table}");

            if (! array_key_exists('use_unique_keys', $this->table_map[$this->table]) || $this->table_map[$this->table]['use_unique_keys'] === TRUE) {
                $this->singer->writeMeta(['unique_keys' => $this->table_map[$this->table]['indexes']]);
            }

            $start_date = new DateTime($this->singer->config->state->bookmarks->{$this->table}->last_started);
            $start_date->sub(new DateInterval("PT5M"));
            $number_of_rows_synced_for_table = 0;

            $this->singer->logger->debug("last sync started date", [$start_date]);

            switch ($this->table) {
                case 'lists':
                    $total_received = 0;
                    while (true) {
                        $response = $this->requestWithRetries(
                            "GET",
                            "/lists",
                            [
                                'since_date_created' => $start_date->format('c'),
                                'sort_field' => 'date_created',
                                'sort_dir' => 'ASC',
                                'count' => 1000,
                                'offset' => $total_received,
                            ]
                        );

                        $list_ids = $this->_processLists($response);

                        if (count($list_ids) == 0) {
                            break;
                        }

                        $total_received = $total_received + count($list_ids);
                    }

                    $number_of_rows_synced_for_table = $total_received;

                    break;

                case 'list_members':
                    $all_list_ids = [];
                    while (true) {
                        $response = $this->requestWithRetries(
                            "GET",
                            "/lists",
                            [
                                'sort_field' => 'date_created',
                                'sort_dir' => 'ASC',
                                'count' => 1000,
                                'offset' => count($all_list_ids),
                            ]
                        );
                        
                        $lists = $response['lists'];

                        if (count($lists) == 0) {
                            break;
                        }

                        foreach ($lists as $list) {
                            $all_list_ids[] = $list['id'];
                        }
                    }

                    $all_list_ids = array_unique($all_list_ids);

                    foreach ($all_list_ids as $list_id) {
                        $total_received = 0;
                        while (true) {
                            $response = $this->requestWithRetries(
                                "GET",
                                "/lists/{$list_id}/members",
                                [
                                    'list_id' => $list_id,
                                    'since_last_changed' => $start_date->format('c'),
                                    'sort_field' => 'last_changed',
                                    'sort_dir' => 'ASC',
                                    'count' => 1000,
                                    'offset' => $total_received,
                                ]
                            );

                            $list_member_ids = $this->_processListMembers($response);

                            if (count($list_member_ids) == 0) {
                                break;
                            }

                            $total_received = $total_received + count($list_member_ids);
                        }

                        $number_of_rows_synced_for_table += $total_received;
                    }

                    break;

                case 'list_segments':
                    $all_list_ids = [];
                    while (true) {
                        $response = $this->requestWithRetries(
                            "GET",
                            "/lists",
                            [
                                'sort_field' => 'date_created',
                                'sort_dir' => 'ASC',
                                'count' => 1000,
                                'offset' => count($all_list_ids),
                            ]
                        );
                        
                        $lists = $response['lists'] ?? [];

                        if (count($lists) == 0) {
                            break;
                        }

                        foreach ($lists as $list) {
                            $all_list_ids[] = $list['id'];
                        }
                    }

                    $all_list_ids = array_unique($all_list_ids);

                    foreach ($all_list_ids as $list_id) {
                        $total_received = 0;
                        while (true) {
                            $response = $this->requestWithRetries(
                                "GET",
                                "/lists/{$list_id}/segments",
                                [
                                    'since_updated_at' => $start_date->format('c'),
                                    'count' => 200, // Mailchimp would 500 if we ask for more than 200 in production
                                    'offset' => $total_received,
                                ]
                            );

                            $list_segment_ids = $this->_processListSegments($response);

                            if (count($list_segment_ids) == 0) {
                                break;
                            }

                            $total_received = $total_received + count($list_segment_ids);
                        }

                        $number_of_rows_synced_for_table += $total_received;
                    }

                    break;

                case 'list_segment_members':
                    $all_list_ids = [];
                    while (true) {
                        $response = $this->requestWithRetries(
                            "GET",
                            "/lists",
                            [
                                'sort_field' => 'date_created',
                                'sort_dir' => 'ASC',
                                'count' => 1000,
                                'offset' => count($all_list_ids),
                            ]
                        );
                        
                        $lists = $response['lists'] ?? [];

                        if (count($lists) == 0) {
                            break;
                        }

                        foreach ($lists as $list) {
                            $all_list_ids[] = $list['id'];
                        }
                    }

                    $all_list_ids = array_unique($all_list_ids);

                    $all_list_segment_ids = [];
                    foreach ($all_list_ids as $list_id) {
                        $total_received = 0;
                        while (true) {
                            $response = $this->requestWithRetries(
                                "GET",
                                "/lists/{$list_id}/segments",
                                [
                                    'count' => 200, // Mailchimp would 500 if we ask for more than 200 in production
                                    'offset' => $total_received,
                                ]
                            );

                            $segments = $response['segments'] ?? [];
                            
                            if (count($segments) == 0) {
                                break;
                            }

                            $total_received = $total_received + count($segments);

                            foreach ($segments as $segment) {
                                $all_list_segment_ids[$list_id][] = $segment['id'];
                            }
                        }
                        $all_list_segment_ids[$list_id] = array_unique($all_list_segment_ids[$list_id]);
                    }

                    foreach ($all_list_segment_ids as $list_id => $segment_ids) {
                        foreach($segment_ids as $segment_id) {
                            $total_received = 0;
                            while (true) {
                                $response = $this->requestWithRetries(
                                    "GET",
                                    "/lists/{$list_id}/segments/{$segment_id}/members",
                                    [
                                        'count' => 1000,
                                        'offset' => $total_received,
                                    ]
                                );

                                $list_segment_member_ids = $this->_processListSegmentMembers($response, $segment_id);

                                if (count($list_segment_member_ids) == 0) {
                                    break;
                                }

                                $total_received = $total_received + count($list_segment_member_ids);
                            }

                            $number_of_rows_synced_for_table += $total_received;
                        }
                    }

                    break;

                case 'campaigns':
                    $total_received = 0;
                    while (true) {
                        $response = $this->requestWithRetries(
                            "GET",
                            "/campaigns",
                            [
                                'since_create_time' => $start_date->format('c'),
                                'sort_field' => 'create_time',
                                'sort_dir' => 'ASC',
                                'count' => 1000,
                                'offset' => $total_received,
                            ]
                        );

                        $campaign_ids = $this->_processCampaigns($response);

                        if (count($campaign_ids) == 0) {
                            break;
                        }

                        $total_received = $total_received + count($campaign_ids);
                    }

                    $number_of_rows_synced_for_table = $total_received;

                    break;

                case 'campaign_unsubscribes':
                    $all_campaign_ids = [];
                    while (true) {
                        $response = $this->requestWithRetries(
                            "GET",
                            "/campaigns",
                            [
                                'sort_field' => 'create_time',
                                'sort_dir' => 'ASC',
                                'count' => 1000,
                                'offset' => count($all_campaign_ids),
                            ]
                        );
                        
                        $campaigns = $response['campaigns'] ?? [];

                        if (count($campaigns) == 0) {
                            break;
                        }

                        foreach ($campaigns as $campaign) {
                            $all_campaign_ids[] = $campaign['id'];
                        }
                    }

                    $all_campaign_ids = array_unique($all_campaign_ids);

                    foreach ($all_campaign_ids as $campaign_id) {
                        $total_received = 0;
                        while (true) {
                            $response = $this->requestWithRetries(
                                "GET",
                                "/reports/{$campaign_id}/unsubscribed",
                                [
                                    'count' => 1000,
                                    'offset' => $total_received,
                                ]
                            );

                            $campaign_unsubscribe_ids = $this->_processCampaignUnsubscribes($response);

                            if (count($campaign_unsubscribe_ids) == 0) {
                                break;
                            }

                            $total_received = $total_received + count($campaign_unsubscribe_ids);
                        }

                        $number_of_rows_synced_for_table += $total_received;
                    }

                    break;

                case 'campaign_email_activity':
                    $all_campaign_ids = [];
                    while (true) {
                        $response = $this->requestWithRetries(
                            "GET",
                            "/campaigns",
                            [
                                'sort_field' => 'create_time',
                                'sort_dir' => 'ASC',
                                'count' => 1000,
                                'offset' => count($all_campaign_ids),
                            ]
                        );
                        
                        $campaigns = $response['campaigns'] ?? [];

                        if (count($campaigns) == 0) {
                            break;
                        }

                        foreach ($campaigns as $campaign) {
                            $all_campaign_ids[] = $campaign['id'];
                        }
                    }

                    $all_campaign_ids = array_unique($all_campaign_ids);
                    
                    // chunk campaign ids into 100 subarrays then run the 100 batches altogether.
                    $batch_size = intval(ceil(count($all_campaign_ids) / 100));
                    $campaign_id_batches = array_chunk($all_campaign_ids, $batch_size);

                    // Make Batch operation requests
                    $batch_ids = [];
                    foreach ($campaign_id_batches as $campaign_id_batch) {
                        $this->singer->logger->debug("Requesting batch operation for campaigns:", [$campaign_id_batch]);

                        $batch_operations = [];
                        foreach ($campaign_id_batch as $campaign_id) {
                            $batch_operations[] = [
                                'method'       => "GET",
                                'path'         => "/reports/{$campaign_id}/email-activity",
                                'operation_id' => "report-{$campaign_id}"
                            ];
                        }

                        $response = $this->requestWithRetries(
                            "POST",
                            "/batches",
                            [],
                            [
                                'operations' => $batch_operations
                            ]
                        );
                        $batch_id = $response['id'];
                        $batch_ids[] = $batch_id;
                    }
                    $this->singer->logger->debug("Submitted batch requests", [$batch_ids]);

                    // Check the status of batch operations every 30 seconds until all the operations are finished
                    $total_received = 0;
                    do {
                        sleep(30);

                        foreach ($batch_ids as $key => $batch_id) {
                            $response = $this->requestWithRetries(
                                "GET",
                                "/batches/{$batch_id}"
                            );
                            $status = $response['status'];
                            $this->singer->logger->debug("batch id: {$batch_id}, status: {$status}");

                            if ( $status == "finished" ) {
                                if ( isset($response['response_body_url']) ) {
                                    $batch_archive_url = $response['response_body_url'];
                                    $this->singer->logger->debug("Extracting data from batch result archive. URL: {$batch_archive_url}");

                                    $results = $this->processBatchArchive($batch_id, $batch_archive_url);

                                    $this->singer->logger->debug("number of records received", [count($results)]);
                                    $total_received = $total_received + count($results);

                                    // Unset finished batch_id from the status check list
                                    unset($batch_ids[$key]);
                                } else {
                                    $this->singer->logger->debug("batch operation failed. id: {$batch_id}");
                                    continue;
                                }
                            }
                        }
                    } while ( count($batch_ids) > 0 );

                    $number_of_rows_synced_for_table += $total_received;

                    break;

                default:
                    throw new Exception("An invalid table name: {$this->table} was provided.");
            }

            $this->singer->logger->debug("Finished sync for {$this->table}");

            $this->singer->writeMetric(
                'counter',
                'record_count',
                $number_of_rows_synced_for_table,
                ['stream' => $this->table]
            );

            $last_started = date(DateTime::ATOM);
            $this->singer->writeState([
                $this->table => [
                    'last_started' => $last_started,
                ]
            ]);
        }
    }

    /**
     * Extract data from gzipped archive, return as array
     */
    public function processBatchArchive($batch_id, $batch_archive_url)
    {
        if ( !is_dir("./batches") ) {
            mkdir("batches");       
        }
        $batch_dir_path = "./batches/{$batch_id}";
        $batch_tar_path = "./batches/{$batch_id}.tar";
        $batch_zip_path = "./batches/{$batch_id}.zip";

        // Get batch process response and save to tar archive.
        $client = new GuzzleHttp\Client();
        $client->request('GET', $batch_archive_url, ['sink' => $batch_tar_path]);

        // Convert the tar archive file to zip format and extract.
        $phar = new PharData($batch_tar_path, RecursiveDirectoryIterator::SKIP_DOTS);
        $phar->convertToData(Phar::ZIP);
        $zip = new ZipArchive;
        $res = $zip->open($batch_zip_path);
        if ($res === TRUE) {
            $zip->extractTo($batch_dir_path);
            $zip->close();
        }

        // process the records.
        $results = [];
        if ( is_dir($batch_dir_path) ) {
            $batch_files = array_diff(scandir($batch_dir_path), array('.', '..'));
            foreach ($batch_files as $batch_file) {
                $batch_data = json_decode(file_get_contents("{$batch_dir_path}/{$batch_file}"));
                if ( count($batch_data) > 0 ) {
                    $batch_data = (array) $batch_data[0];
                    if ( isset($batch_data['response']) ) {
                        $response = (array) json_decode($batch_data['response']);

                        $campaign_email_activity_ids = $this->_processCampaignEmailActivity($response);
                        $results = array_merge($results, $campaign_email_activity_ids);
                    }
                }
            }
        }

        // Delete batch files
        if ( file_exists($batch_zip_path) ) {
            unlink($batch_zip_path);
        }
        if ( file_exists($batch_tar_path) ) {
            unlink($batch_tar_path);
        }
        if ( is_dir($batch_dir_path) ) {
            $files = glob($batch_dir_path . '/*');
            foreach ($files as $file) {
                if (is_file($file)) {
                    unlink($file);
                }
            }
            rmdir($batch_dir_path);
        }

        // Return array of processed record IDs
        return $results;
    }

    /**
     * Writes a metadata response with the tables to STDOUT
     */
    public function getTables()
    {
        $tables = array_values(array_keys($this->table_map));
        $this->singer->writeMeta(compact('tables'));
    }

    /**
     * Processes the HTTP response for /lists/
     *
     * @param array   $response    Array of response data
     *
     * @return array
     */
    function _processLists($response)
    {
        if ( isset($response['lists']) ) {
            $lists = $response['lists'];
            $ids = [];
            $records = [];

            foreach ($lists as $list) {
                $table_columns = $this->table_map['lists']['properties'];
                $list = $this->formatRecord($list, $table_columns);

                $ids[] = (string) $list['id'];

                $records[] = [
                    'id' => $list['id'],
                    'web_id' => $list['web_id'],
                    'name' => $list['name'],
                    'contact' => json_encode($list['contact']),
                    'permission_reminder' => $list['permission_reminder'],
                    'use_archive_bar' => $list['use_archive_bar'],
                    'campaign_defaults' => json_encode($list['campaign_defaults']),
                    'notify_on_subscribe' => json_encode($list['notify_on_subscribe']),
                    'notify_on_unsubscribe' => json_encode($list['notify_on_unsubscribe']),
                    'date_created' => $this->formatTimestamp($list['date_created']),
                    'list_rating' => $list['list_rating'],
                    'email_type_option' => $list['email_type_option'],
                    'subscribe_url_short' => $list['subscribe_url_short'],
                    'subscribe_url_long' => $list['subscribe_url_long'],
                    'beamer_address' => $list['beamer_address'],
                    'visibility' => $list['visibility'],
                    'double_optin' => $list['double_optin'],
                    'has_welcome' => $list['has_welcome'],
                    'marketing_permissions' => $list['marketing_permissions'],
                    'modules' => json_encode($list['modules']),
                    'stats' => json_encode($list['stats']),
                    '_links' => json_encode($list['_links'])
                ];
            }

            if ( ! empty($ids) ) {
                $this->_deleteRecords($ids);
            }

            $this->_insertRecords($records);
            return $ids;
        }
        return [];
    }

    /**
     * Processes the HTTP response for /list/x/members
     *
     * @param array   $response    Array of response data
     *
     * @return array
     */
    function _processListMembers($response) {
        if ( isset($response['members']) ) {
            $list_members = $response['members'];
            $records = [];
            $ids = [];

            foreach ($list_members as $list_member) {
                $table_columns = $this->table_map['list_members']['properties'];
                $list_member = $this->formatRecord($list_member, $table_columns);

                $ids[] = (string) $list_member['id'];

                $records[] = [
                    'id' =>  $list_member['id'],
                    'list_id' =>  $list_member['list_id'],
                    'email_address' => $list_member['email_address'],
                    'unique_email_id' => $list_member['unique_email_id'],
                    'contact_id' => $list_member['contact_id'],
                    'full_name' => $list_member['full_name'],
                    'web_id' => $list_member['web_id'],
                    'email_type' => $list_member['email_type'],
                    'status' => $list_member['status'],
                    'unsubscribe_reason' => $list_member['unsubscribe_reason'],
                    'consents_to_one_to_one_messaging' => $list_member['consents_to_one_to_one_messaging'],
                    'merge_fields' => json_encode($list_member['merge_fields']),
                    'interests' => json_encode($list_member['interests']),
                    'stats' => json_encode($list_member['stats']),
                    'ip_signup' => $list_member['ip_signup'],
                    'timestamp_signup' => $this->formatTimestamp($list_member['timestamp_signup']),
                    'ip_opt' => $list_member['ip_opt'],
                    'timestamp_opt' => $this->formatTimestamp($list_member['timestamp_opt']),
                    'member_rating' => $list_member['member_rating'],
                    'last_changed' => $this->formatTimestamp($list_member['last_changed']),
                    'language' => $list_member['language'],
                    'vip' => $list_member['vip'],
                    'email_client' => $list_member['email_client'],
                    'location' => json_encode($list_member['location']),
                    'marketing_permissions' => json_encode($list_member['marketing_permissions']),
                    'last_note' => json_encode($list_member['last_note']),
                    'source' => $list_member['source'],
                    'tags_count' => $list_member['tags_count'],
                    'tags' => json_encode($list_member['tags']),
                    '_links' => json_encode($list_member['_links'])
                ];
            }

            $this->_insertRecords($records);
            return $ids;
        }
        return [];
    }

    /**
     * Processes the HTTP response for /list/x/segments
     *
     * @param array   $response    Array of response data
     *
     * @return array
     */
    function _processListSegments($response)
    {
        if ( isset($response['segments']) ) {
            $list_segments = $response['segments'];
            $records = [];
            $ids = [];

            foreach ($list_segments as $list_segment) {
                $table_columns = $this->table_map['list_segments']['properties'];
                $list_segment = $this->formatRecord($list_segment, $table_columns);

                $ids[] = (string) $list_segment['id'];

                $records[] = [
                    'id' => $list_segment['id'],
                    'list_id' => $list_segment['list_id'],
                    'name' => $list_segment['name'],
                    'member_count' => $list_segment['member_count'],
                    'type' => $list_segment['type'],
                    'created_at' => $this->formatTimestamp($list_segment['created_at']),
                    'updated_at' => $this->formatTimestamp($list_segment['updated_at']),
                    'options' => json_encode($list_segment['options']),
                    '_links' => json_encode($list_segment['_links'])
                ];
            }

            $this->_insertRecords($records);
            return $ids;
        }
        return [];
    }

    /**
     * Processes the HTTP response for /list/x/segment/x/members
     *
     * @param array   $response    Array of response data
     *
     * @return array
     */
    function _processListSegmentMembers($response, $segment_id)
    {
        if ( isset($response['members']) ) {
            $list_segment_members = $response['members'];
            $records = [];
            $ids = [];

            foreach($list_segment_members as $list_segment_member) {
                $table_columns = $this->table_map['list_segment_members']['properties'];
                $list_segment_member = $this->formatRecord($list_segment_member, $table_columns);

                $ids[] = (string) $list_segment_member['id'];

                $records[] = [
                    'id' => $list_segment_member['id'],
                    'list_id' => $list_segment_member['list_id'],
                    'segment_id' => $segment_id,
                    'email_address' => $list_segment_member['email_address'],
                    'unique_email_id' => $list_segment_member['unique_email_id'],
                    'email_type' => $list_segment_member['email_type'],
                    'status' => $list_segment_member['status'],
                    'merge_fields' => json_encode($list_segment_member['merge_fields']),
                    'interests' => json_encode($list_segment_member['interests']),
                    'stats' => json_encode($list_segment_member['stats']),
                    'ip_signup' => $list_segment_member['ip_signup'],
                    'timestamp_signup' => $this->formatTimestamp($list_segment_member['timestamp_signup']),
                    'ip_opt' => $list_segment_member['ip_opt'],
                    'timestamp_opt' => $this->formatTimestamp($list_segment_member['timestamp_opt']),
                    'member_rating' => $list_segment_member['member_rating'],
                    'last_changed' => $this->formatTimestamp($list_segment_member['last_changed']),
                    'language' => $list_segment_member['language'],
                    'vip' => $list_segment_member['vip'],
                    'email_client' => $list_segment_member['email_client'],
                    'location' => json_encode($list_segment_member['location']),
                    'last_note' => json_encode($list_segment_member['last_note']),
                    '_links' => json_encode($list_segment_member['_links'])
                ];
            }

            $this->_insertRecords($records);
            return $ids;
        }
        return [];
    }

    /**
     * Processes the HTTP response for /campaigns
     *
     * @param array   $response    Array of response data
     *
     * @return array
     */
    function _processCampaigns($response)
    {
        if ( isset($response['campaigns']) ) {
            $campaigns = $response['campaigns'];
            $records = [];
            $ids = [];

            foreach($campaigns as $campaign) {
                $table_columns = $this->table_map['campaigns']['properties'];
                $campaign = $this->formatRecord($campaign, $table_columns);

                $ids[] = (string) $campaign['id'];

                $records[] = [
                    'id' => $campaign['id'],
                    'web_id' => $campaign['web_id'],
                    'parent_campaign_id' => $campaign['parent_campaign_id'],
                    'type' => $campaign['type'],
                    'create_time' => $this->formatTimestamp($campaign['create_time']),
                    'archive_url' => $campaign['archive_url'],
                    'long_archive_url' => $campaign['long_archive_url'],
                    'status' => $campaign['status'],
                    'emails_sent' => $campaign['emails_sent'],
                    'send_time' => $this->formatTimestamp($campaign['send_time']),
                    'content_type' => $campaign['content_type'],
                    'needs_block_refresh' => $campaign['needs_block_refresh'],
                    'resendable' => $campaign['resendable'],
                    'recipients' => json_encode($campaign['recipients']),
                    'settings' => json_encode($campaign['settings']),
                    'variate_settings' => json_encode($campaign['variate_settings']),
                    'tracking' => json_encode($campaign['tracking']),
                    'rss_opts' => json_encode($campaign['rss_opts']),
                    'ab_split_opts' => json_encode($campaign['ab_split_opts']),
                    'social_card' => json_encode($campaign['social_card']),
                    'report_summary' => json_encode($campaign['report_summary']),
                    'delivery_status' => json_encode($campaign['delivery_status']),
                    '_links' => json_encode($campaign['_links'])
                ];
            }

            $this->_insertRecords($records);
            return $ids;
        }
        return [];
    }

    /**
     * Processes the HTTP response for /reports/x/unsubscribes
     *
     * @param array   $response    Array of response data
     *
     * @return array
     */
    function _processCampaignUnsubscribes($response)
    {
        if ( isset($response['unsubscribes']) ) {
            $campaign_unsubscribes = $response['unsubscribes'];
            $records = [];
            $ids = [];

            foreach($campaign_unsubscribes as $campaign_unsubscribe) {
                $table_columns = $this->table_map['campaign_unsubscribes']['properties'];
                $campaign_unsubscribe = $this->formatRecord($campaign_unsubscribe, $table_columns);

                $ids[] = (string) $campaign_unsubscribe['email_id'];

                $records[] = [
                    'campaign_id' => $campaign_unsubscribe['campaign_id'],
                    'email_id' => $campaign_unsubscribe['email_id'],
                    'email_address' => $campaign_unsubscribe['email_address'],
                    'merge_fields' => json_encode($campaign_unsubscribe['merge_fields']),
                    'vip' => $campaign_unsubscribe['vip'],
                    'timestamp' => $this->formatTimestamp($campaign_unsubscribe['timestamp']),
                    'reason' => $campaign_unsubscribe['reason'],
                    'list_id' => $campaign_unsubscribe['list_id'],
                    'list_is_active' => $campaign_unsubscribe['list_is_active'],
                    '_links' => json_encode($campaign_unsubscribe['_links'])
                ];
            }

            $this->_insertRecords($records);
            return $ids;
        }
        return [];
    }

    /**
     * Processes the HTTP response for /reports/x/email-activity
     *
     * @param array   $response    Array of response data
     *
     * @return array
     */
    function _processCampaignEmailActivity($response)
    {
        if ( isset($response['emails']) ) {
            $activities = $response['emails'];
            $records = [];
            $ids = [];

            foreach($activities as $activity) {
                $activity = (array) $activity;
                if (! empty($activity['activity'])) {
                    $table_columns = $this->table_map['campaign_email_activity']['properties'];
                    $activity = $this->formatRecord($activity, $table_columns);

                    $ids[] = (string) $activity['email_id'];

                    $records[] = [
                        'campaign_id' => $activity['campaign_id'],
                        'list_id' => $activity['list_id'],
                        'list_is_active' => $activity['list_is_active'],
                        'email_id' => $activity['email_id'],
                        'email_address' => $activity['email_address'],
                        'activity' => json_encode($activity['activity']),
                        '_links' => json_encode($activity['_links'])
                    ];
                }
            }

            $this->_insertRecords($records);
            return $ids;
        }
        return [];
    }

    /**
     * Handles deleting records from the database.
     *
     * @param array   $ids     Array of record ids to delete
     *
     * @return void
     */
    function _deleteRecords($ids)
    {
        foreach ($ids as $id) {
            $this->singer->writeDeleteRecord(
                stream: $this->table,
                record: ['id' => $id],
                soft_delete: true,
            );
        }
    }

    /**
     * Handles inserting records into the database.
     *
     * @param array   $records     Array of records to insert
     *
     * @return void
     */
    function _insertRecords($records)
    {
        foreach ($records as $record) {
            $this->singer->writeRecord(
                stream: $this->table,
                record: $record
            );
        }
    }

    /**
     * Format records to match table columns
     *
     * @param array   $record           The response array
     * @param array   $columns          The record model
     *
     * @return array
     */
    public function formatRecord($record, $columns) {
        // Remove unmapped fields from the response.
        $record = array_filter($record, function($key) use($columns) {
            return array_key_exists($key, $columns);
        }, ARRAY_FILTER_USE_KEY);

        // column mapping for missing response fields.
        foreach ($columns as $colKey => $colVal) {
            if (!array_key_exists($colKey, $record)) {
                $record[$colKey] = null;
            }
        }

        return $record;
    }

    /**
     * Format timestamp
     */
    function formatTimestamp(string $timestamp)
    {
        try {
            $dtm = new DateTime($timestamp);
        } catch (Exception $err) {
            return NULL;
        }

        return $dtm->format(DateTime::ATOM);
    }

    /**
     * Query the Mailchimp APIs with retries.
     *
     * @param string  $method       The API request method  GET | POST
     * @param string  $uri          The URI for the REST query
     * @param array   $params       An array of URL parameters
     * @param array   $payload      The POST request payload
     *
     * @return array
     */
    public function requestWithRetries($method, $uri, $params = [], $payload = [])
    {
        $attempts = 1;
        while(true) {
            try {
                return $this->request($method, $uri, $params, $payload);
            } catch (Exception $e) {
                if ($attempts > $this->max_retries) {
                    throw $e;
                }
                $this->singer->logger->debug("Request failed. Retrying in {$this->retry_delay} seconds. ({$attempts}/{$this->max_retries})");
                $attempts++;
                sleep($this->retry_delay);
            }
        }
    }

    /**
     * Query the Mailchimp APIs.
     *
     * @param string  $method       The API request method  GET | POST
     * @param string  $uri          The URI for the REST query
     * @param array   $params       An array of URL parameters
     * @param array   $payload      The POST request payload
     *
     * @return array
     */
    public function request($method, $uri, $params = [], $payload = [])
    {
        $client = new GuzzleHttp\Client();
        $url = "https://{$this->dataCenter}.api.mailchimp.com/3.0{$uri}";

        $response = $client->request(
            $method,
            $url,
            [
                'query' => $params,
                'headers' => [ "Authorization" => "Bearer {$this->token}"],
                'http_errors' => false,
                'body' => json_encode($payload)
            ]
        );

        $code = $response->getStatusCode();
        switch ($code) {
            case 429:
                $this->singer->logger->debug("Mailchimp has asked us to slow down. We'll wait 90 seconds before making another request.");
                sleep($this->rate_limit_delay);
                return $this->request($method, $uri, $params, $payload);
            case 400:
                $json_body = json_decode((string) $response->getBody(), true);
                if (is_array($json_body) && array_key_exists('detail', $json_body)) {
                    $url .= '?' . http_build_query($params);
                    $this->singer->writeInfo("When requesting {$url}, Mailchimp responded with a 400 status error: " . $json_body['detail']);
                    $this->singer->logger->error("Guzzle Mailchimp Error", compact('url', 'json_body'));
                    return [];
                }

                throw new Exception("An unhandled HTTP response has occurred with a status code of ({$code}).");
            case 200:
                $body = json_decode((string) $response->getBody(), true);
                return $body;
            default:
                throw new Exception("An unhandled HTTP response has occurred with a status code of ({$code}).");
        }
    }

    private $table_map = [
        'lists' => [
            'indexes' => [
                'id'
            ],
            'properties' => [
                'id' => 'string',
                'web_id' => 'string',
                'name' => 'string',
                'contact' => 'object',
                'permission_reminder' => 'string',
                'use_archive_bar' => 'boolean',
                'campaign_defaults' => 'object',
                'notify_on_subscribe' => 'object',
                'notify_on_unsubscribe' => 'object',
                'date_created' => 'timestampz',
                'list_rating' => 'int',
                'email_type_option' => 'boolean',
                'subscribe_url_short' => 'string',
                'subscribe_url_long' => 'string',
                'beamer_address' => 'string',
                'visibility' => 'string',
                'double_optin' => 'boolean',
                'has_welcome' => 'boolean',
                'marketing_permissions' => 'boolean',
                'modules' => 'object',
                'stats' => 'object',
                '_links' => 'object'
            ],
        ],
        'list_members' => [
            'indexes' => [
                'id',
                'list_id',
                'unique_email_id',
                'contact_id'
            ],
            'properties' => [
                'id' => 'string',
                'list_id' => 'string',
                'email_address' => 'string',
                'unique_email_id' => 'string',
                'contact_id' => 'string',
                'full_name' => 'string',
                'web_id' => 'int',
                'email_type' => 'string',
                'status' => 'string',
                'unsubscribe_reason' => 'string',
                'consents_to_one_to_one_messaging' => 'boolean',
                'merge_fields' => 'object',
                'interests' => 'object',
                'stats' => 'object',
                'ip_signup' => 'string',
                'timestamp_signup' => 'timestampz',
                'ip_opt' => 'string',
                'timestamp_opt' => 'timestampz',
                'member_rating' => 'int',
                'last_changed' => 'timestampz',
                'language' => 'string',
                'vip' => 'boolean',
                'email_client' => 'string',
                'location' => 'object',
                'marketing_permissions' => 'object',
                'last_note' => 'object',
                'source' => 'string',
                'tags_count' => 'int',
                'tags' => 'object',
                '_links' => 'object'
            ],
        ],
        'list_segments' => [
            'indexes' => [
                'id',
                'list_id'
            ],
            'properties' => [
                'id' => 'string',
                'list_id' => 'string',
                'name' => 'string',
                'member_count' => 'int',
                'type' => 'string',
                'created_at' => 'timestampz',
                'updated_at' => 'timestampz',
                'options' => 'object',
                '_links' => 'object'
            ],
        ],
        'list_segment_members' => [
            'indexes' => [
                'id',
                'list_id',
                'unique_email_id'
            ],
            'properties' => [
                'id' => 'string',
                'list_id' => 'string',
                'segment_id' => 'string',
                'email_address' => 'string',
                'unique_email_id' => 'string',
                'email_type' => 'string',
                'status' => 'string',
                'merge_fields' => 'object',
                'interests' => 'object',
                'stats' => 'object',
                'ip_signup' => 'string',
                'timestamp_signup' => 'timestampz',
                'ip_opt' => 'string',
                'timestamp_opt' => 'timestampz',
                'member_rating' => 'int',
                'last_changed' => 'timestampz',
                'language' => 'string',
                'vip' => 'boolean',
                'email_client' => 'string',
                'location' => 'object',
                'last_note' => 'object',
                '_links' => 'object'
            ],
            'use_unique_keys' => FALSE,
        ],
        'campaigns' => [
            'indexes' => [
                'id',
                'parent_campaign_id'
            ],
            'properties' => [
                'id' => 'string',
                'web_id' => 'int',
                'parent_campaign_id' => 'string',
                'type' => 'string',
                'create_time' => 'timestampz',
                'archive_url' => 'string',
                'long_archive_url' => 'string',
                'status' => 'string',
                'emails_sent' => 'int',
                'send_time' => 'timestampz',
                'content_type' => 'string',
                'needs_block_refresh' => 'boolean',
                'resendable' => 'boolean',
                'recipients' => 'object',
                'settings' => 'object',
                'variate_settings' => 'object',
                'tracking' => 'object',
                'rss_opts' => 'object',
                'ab_split_opts' => 'object',
                'social_card' => 'object',
                'report_summary' => 'object',
                'delivery_status' => 'object',
                '_links' => 'object'
            ],
        ],
        'campaign_unsubscribes' => [
            'indexes' => [
                'campaign_id',
                'list_id',
                'email_id'
            ],
            'properties' => [
                'campaign_id' => 'string',
                'email_id' => 'string',
                'email_address' => 'string',
                'merge_fields' => 'object',
                'vip' => 'boolean',
                'timestamp' => 'timestampz',
                'reason' => 'string',
                'list_id' => 'string',
                'list_is_active' => 'boolean',
                '_links' => 'object'
            ],
            'use_unique_keys' => FALSE, // unsubscribes CAN have duplicates from Mailchimp. Don't try to enforce uniqueness.
        ],
        'campaign_email_activity' => [
            'indexes' => [
                'campaign_id',
                'list_id',
                'email_id'
            ],
            'properties' => [
                'campaign_id' => 'string',
                'list_id' => 'string',
                'list_is_active' => 'boolean',
                'email_id' => 'string',
                'email_address' => 'string',
                'activity' => 'object',
                '_links' => 'object'
            ],
        ]
    ];
}
