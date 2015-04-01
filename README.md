worker-download
==============

[![Build Status](https://travis-ci.org/photon/worker-download.svg?branch=master)](https://travis-ci.org/photon/worker-download)

A worker to stream just-in-time a HTTP Response from a iterator

Quick start
-----------

1) Add the module in your project

    composer require "photon/worker-download"

2) Add the worker in the configuration

    'installed_tasks' => array(
        'PhotonDownload'            =>  '\photon\task\PhotonDownload',
    ),
    'photon_task_PhotonDownload' => array(
        'sub_addr' => 'tcp://127.0.0.1:11011',
        'm2ControlAddress' => 'tcp://127.0.0.1:9999',
    ),

3) Start the worker

    hnu worker PhotonDownload

4) Send work to the worker in a view

	public function m2stream($request, $match)
	{
        $ans = new \photon\http\Response(new MyStream);
        $ans->headers['Content-Length'] = 247463936;

        \photon\task\PhotonDownload::createDownload($request, $ans);
        return false;
    }

