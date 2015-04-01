<?php

use \photon\http\Response;
use \photon\task\PhotonDownload;

class PhotonDownloadTest extends \photon\test\TestCase
{
    public function testStringContent()
    {
        $this->setExpectedException('photon\task\Exception');

        $req = \photon\test\HTTP::baseRequest();
        $res = new Response('A String');
        PhotonDownload::createDownload($req, $res);
    }
}
