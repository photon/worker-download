<?php

namespace photon\task;
use \photon\config\Container as Conf;
use \photon\log\Log;
use photon\mongrel2\Connection;

/*
 *  A worker to stream just-in-time a HTTP Response from a iterator
 *  It's avoid the fill the RAM of the server
 *
 *  Technical Note :
 *      Mongrel2 will kill the HTTP connection, if the application handler send
 *      too much data chunck. The actual mongrel2 limit is 16 chuncks. So sending
 *      too small chunck can have a negative impact on the download bandwidth. 
 */
class PhotonDownload extends SyncTask
{
    const MAX_CHUNCK = 16;
    private $connections = array();
    private $jobs = array();

    public function __construct($conf = array())
    {
        parent::__construct($conf);

        // Connect to each mongrel2 servers on port PUB & Control only
        // The worker do not want to receive HTTP requests from mongrel2
        $servers = Conf::f('server_conf', null);
        if ($servers === null) {
            Log::fatal("No mongrel2 servers in the configuration");
            Log::flush();
            exit(0);
        }
    
        foreach ($servers as $serverId => $server) {
            // Ignore servers without control port
            if (isset($server['ctrl_addr']) === false || $server['ctrl_addr'] === null ||
                isset($server['pub_addr']) === false  || $server['pub_addr'] === null) {
                 continue;
            }

            Log::info('Server #' . $serverId . ' Control = ' . $server['ctrl_addr'] . '    Pub = ' . $server['pub_addr']);
            $connection = new Connection(null, $server['pub_addr'], $server['ctrl_addr']);
            $connection->connect();
            $this->connections[] = $connection;
            $this->jobs[] = array();
        }
        if (count($this->connections) === 0) {
            Log::fatal('No mongrel2 servers with control port detected');
            Log::flush();
            exit(0);
        }

        Log::info('Task ready, ' . gmdate('c'));
    }

    /*
     *  Send a message to the task to perform the requested download,
     *  The iterator will be serialized and unserialize.
     */
    static public function createDownload($request, $response)
    {
        // Ensure the response content is a iterator
        if (is_array($response->content) === false && ($response->content instanceof \Traversable) === false) {
            throw new Exception('The content of the reponse is not iterable');
        }

        $payload = array(
            'action' => 'download',
            'id' => $request->mess->conn_id,
            'pub' => $request->conn->getPubAddr(),
            'response' => serialize($response),
        );

        $runner = new \photon\task\Runner();
        return $runner->run('PhotonDownload', $payload);
    }

    /*
     * Interface to web handler
     */
    public function work($socket)
    {
        list($taskname, $client, $jmsg) = explode(' ', $socket->recv(), 3);
        $payload = json_decode($jmsg);

        if ($payload->action === 'download') {
            $id = $payload->id;
            $pub = $payload->pub;
            $response = unserialize($payload->response);

            // Ensure the mongrel2 connection exist to process this download
            list($connectionIndex, $connection) = $this->getConnectionFromPubAddr($pub);
            if ($connection === null) {
                $mess = sprintf('%s %s %s', $client, $taskname, json_encode(array(
                    'ok' => false,
                    'msg' => 'Download worker do not known this mongrel2 server',
                )));
                $socket->send($mess);
                return;
            }

            // Send headers and first chunck, background task will send last parts later
            $firstChunck = $response->getHeaders() . "\r\n" . $response->content->current();
            $sz = strlen($firstChunck);
            $connection->send('PhotonDownload', $id, $firstChunck);
            $this->jobs[$connectionIndex][$id] = array(
                'iterator' => $response->content,
                'bytes_written' => 0,
                'bytes_sent' => $sz,
                'active' => true,
                'last_chunck' => array_fill(0, self::MAX_CHUNCK, 0),
            );
            
            array_shift($this->jobs[$connectionIndex][$id]['last_chunck']);
            array_push($this->jobs[$connectionIndex][$id]['last_chunck'], $sz);

            $mess = sprintf('%s %s %s', $client, $taskname, json_encode(array('ok' => true)));
            $socket->send($mess);
            return;
        }

        $mess = sprintf('%s %s %s', $client, $taskname, json_encode(array(
            'ok' => false,
            'msg' => 'Unknown action',
        )));
        $socket->send($mess);
    }

    public function getConnectionFromPubAddr($addr)
    {
        foreach($this->connections as $index => $connection) {
            if ($connection->getPubAddr() === $addr) {
                return array($index, $connection);
            }
        }

        return null;
    }

    /*
     *  Background task, check buffers to fill
     */
    public function loop()
    {
        // Execute the back ground task, only one time per seconds
        static $lastRefresh = 0;
        $now = time();
        if ($now == $lastRefresh) {
            return;
        }
        $lastRefresh = $now;

        foreach($this->connections as $connectionIndex => $connection) {
            $this->_loop($connectionIndex, $connection, $this->jobs[$connectionIndex]);
        }
    }

    public function _loop($connectionIndex, $connection, &$jobs)
    {
        // Refresh statistics from mongrel2
        $tnetstring = $connection->control('26:6:status,13:4:what,3:net,}]');
        if ($tnetstring === null) {
            return;
        }
        $stats = \tnetstring_decode($tnetstring);
        if (isset($stats['rows'][0]) === false) {
            return;
        }
        if (is_array($stats['rows'][0]) === false) {
            $r = $stats['rows'];
            unset($stats['rows']);
            $stats['rows'][0] = $r;
        }
        
        // Mark all jobs as inactive to detect disconnected
        foreach($jobs as $key => $job) {
            if (isset($jobs[$key]) === true) {
                $jobs[$key]['active'] = false;
                $jobs[$key]['bytes_written_last'] = $jobs[$key]['bytes_written'];
            }
        }

        // Update internal statistics
        foreach($stats['rows'] as $row) {
            if ($row[0] === -1) {
                continue;
            }

            if (isset($jobs[$row[0]])) {
                $jobs[$row[0]]['bytes_written'] = $row[7];
                $jobs[$key]['active'] = true;
            }
        }

        // Remove disconnected
        foreach($jobs as $key => $job) {
            if ($job['active'] === false) {
                unset($jobs[$key]);
            }
        }

        // Grow mongrel2 buffer
        foreach($jobs as $key => &$job) {
            // bytes transfered last second per the client
            $size = $job['bytes_written'] - $job['bytes_written_last'];

            // Try to increase the bandwidth, maybe the client can follow             
            $size = ($size < 1024 * 1024 /* 1MB */) ? 1024 * 1024 : $size;
            $overcommit = ($job['bytes_written'] === $job['bytes_sent']) ? 2 : 1;   
            
            // Iterate on the data, we can not known numbers and size of chuncks in the iterator
            do {
                // Ensure the mongrel2 buffer is at least 1MB or the size transfered last seconds
                if ($job['bytes_sent'] - $job['bytes_written'] > ($size * $overcommit)) {
                    break;   // Lot of data available in mongrel2
                }

                // Ensure they have a chunck slot available in mongrel2
                $limit = array_sum($job['last_chunck']) - $job['last_chunck'][0];
                if ($job['bytes_sent'] - $job['bytes_written'] > $limit) {
                    break; // No more slot
                }

                $it = &$job['iterator'];
                $it->next();
                if ($it->valid() === false) {

                    // Notify mongrel2 about the end
                    $connection->send('PhotonDownload', $key, '');

                    // End of the iterator
                    unset($it);
                    unset($this->jobs[$key]);
                    break;
                }
        
                $buffer = $it->current();
                $sz = strlen($buffer);

                $job['bytes_sent'] += $sz;
                array_shift($job['last_chunck']);
                array_push($job['last_chunck'], $sz);

                $connection->send('PhotonDownload', $key, $buffer);
            } while (true);
        }
        unset($job);
    }
}

