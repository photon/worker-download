<?php

namespace photon\task;
use \photon\config\Container as Conf;
use \photon\log\Log;

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
    public $m2ControlAddress = null;

    private $socketControl = null;
    private $socket = null;
    private $jobs = array();

    public function __construct($conf = array())
    {
        parent::__construct($conf);
        var_dump($this->name);

        // Connect to the mongrel2 control port
        $this->socketControl = new \ZMQSocket($this->ctx, \ZMQ::SOCKET_REQ);
        Log::info('Connecting to control port : ' . $this->m2ControlAddress);
        $this->socketControl->connect($this->m2ControlAddress);

        // Connect to the out-going mongrel2 port
        $this->socket = new \ZMQSocket($this->ctx, \ZMQ::SOCKET_PUB);
        $this->socket->setSockOpt(\ZMQ::SOCKOPT_IDENTITY, 'PhotonDownload');
        $servers = Conf::f('server_conf', null);
        if ($servers === null) {
            Log::fatal("No mongrel2 servers in the configuration");
            Log::flush();
            exit(0);
        }
        foreach ($servers['pub_addrs'] as $addr) {
            Log::info('Connecting to PUB addrs : ' . $addr);
            $this->socket->connect($addr);
        }

        Log::info('Task ready, ' . gmdate('c'));
    }

    /*
     *  Send a message to the task to perform the requested download,
     *  The iterator will be serialized and unserialize.
     */
    static public function createDownload($request, $iterator)
    {
        $payload = array(
            'action' => 'download',
            'id' => $request->mess->conn_id,
            'response' => serialize($iterator),
        );

        $runner = new \photon\task\Runner();
        return $runner->run('PhotonDownload', $payload);
    }

    /*
     *  Serialize and send the chunck to the mongrel2
     */
    private function send($id, $chunck)
    {
        $this->socket->send('PhotonDownload' . ' ' . \strlen($id) . ':' . $id . ', ' . $chunck);
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
            $response = unserialize($payload->response);

            // Send headers and first chunck, background task will send last parts later
            $firstChunck = $response->getHeaders() . "\r\n" . $response->content->current();
            $sz = strlen($firstChunck);
            $this->send($id, $firstChunck);
            $this->jobs[$id] = array(
                'iterator' => $response->content,
                'bytes_written' => 0,
                'bytes_sent' => $sz,
                'active' => true,
                'last_chunck' => array_fill(0, self::MAX_CHUNCK, 0),
            );
            
            array_shift($this->jobs[$id]['last_chunck']);
            array_push($this->jobs[$id]['last_chunck'], $sz);

            $mess = sprintf('%s %s %s', $client, $taskname, json_encode(array('ok' => true)));
            $socket->send($mess);
            return;
        }

        $mess = sprintf('%s %s %s', $client, $taskname, json_encode(array('ok' => false)));
        $socket->send($mess);
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

        // Refresh statistics from mongrel2
        $this->socketControl->send('26:6:status,13:4:what,3:net,}]');
        $stats = \tnetstring_decode($this->socketControl->recv());
        if (isset($stats['rows'][0]) === false) {
            return;
        }
        if (is_array($stats['rows'][0]) === false) {
            $r = $stats['rows'];
            unset($stats['rows']);
            $stats['rows'][0] = $r;
        }
        
        // Mark all jobs as inactive to detect disconnected
        foreach($this->jobs as $key => $job) {
            $this->jobs[$key]['active'] = false;
            $this->jobs[$key]['bytes_written_last'] = $this->jobs[$key]['bytes_written'];
        }

        // Update internal statistics
        foreach($stats['rows'] as $row) {
            if ($row[0] === -1) {
                continue;
            }

            if (isset($this->jobs[$row[0]])) {
                $this->jobs[$row[0]]['bytes_written'] = $row[7];
                $this->jobs[$key]['active'] = true;
            }
        }

        // Remove disconnected
        foreach($this->jobs as $key => $job) {
            if ($job['active'] === false) {
                unset($this->jobs[$key]);
            }
        }

        // Grow mongrel2 buffer
        foreach($this->jobs as $key => &$job) {
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

                $this->send($key, $buffer);
            } while (true);
        }
        unset($job);
    }
}

