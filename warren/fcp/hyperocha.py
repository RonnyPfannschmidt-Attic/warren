import miniFCP
import os

DEFAULT_CODEC = 'LZMA_NEW'


def transfer_mimetype_and_name(job, cmd):
    cmdargs = job._cmdargs
    if 'Mimetype' in cmdargs:
        cmd.items["Metadata.ContentType"] = cmdargs['Mimetype']
    if 'TargetFilename' in cmdargs:
        cmd.items["TargetFilename"] = cmdargs['TargetFilename']



class GetConfigJob(miniFCP.FCPJob):
    def __init__(self, WithCurrent=False, WithExpertFlag=False):
        miniFCP.FCPJob.__init__(self)
        self._msg = None
        self.WithCurrent = WithCurrent
        self.WithExpertFlag = WithExpertFlag

    def onMessage(self, msg):
        if msg.name == 'ConfigData':
            self._msg = msg
            self.setSuccess()
        else:
            miniFCP.FCPJob.onMessage(self, msg)

    def getFCPCommand(self):
        cmd = self.makeFCPCommand('GetConfig',dict(
                WithCurrent=self.WithCurrent,
                WithExpertFlag=self.WithExpertFlag))
        return cmd, None

    def getConfig(self):
        return self._msg.items

class PutDirectJob(miniFCP.FCPJob):
    def __init__(self, uri, content, callback, **cmdargs):
        miniFCP.FCPJob.__init__(self)
        self._targetURI = uri
        self._callback = callback
        self._cmdargs = cmdargs
        self._content = content

    def onMessage(self, msg):
        if msg.name == 'SimpleProgress':
            self._callback.onSimpleProgress(msg)
        elif msg.name == 'FinishedCompression':
            self._callback.onFinishedCompression(msg)
        elif msg.name == 'URIGenerated':
            self._callback.onURIGenerated(msg)
        elif msg.name == 'PutFetchable':
            self._callback.onPutFetchable(msg)
        elif msg.name == 'PutSuccessful':
            self.setSuccess()
            self._callback.onSuccess(msg)
        elif msg.name == 'PutFailed':
            self.setErrorMessage(msg)
            self._callback.onFailed(msg)
        else:
            miniFCP.FCPJob.onMessage(self, msg)

    def getFCPCommand(self):
        cmd = self.makeFCPCommand('ClientPut', {
            'URI': self._targetURI,
            'Verbosity': self._cmdargs.get('Verbosity', -1),
            'MaxRetries': self._cmdargs.get('MaxRetries', 3),
            'DontCompress': 'false',
            'Codecs': DEFAULT_CODEC,
            'PriorityClass': self._cmdargs.get('PriorityClass', 1),
            'Global': 'false',
            'Persistence': 'connection',
            "UploadFrom": "direct",
            "DataLength": len(self._content),
            "RealTimeFlag": "true",
        })
        transfer_mimetype_and_name(self, cmd)
        return cmd, self._content

class PutQueueDirectJob(miniFCP.FCPJob):
    def __init__(self, uri, content, **cmdargs):
        miniFCP.FCPJob.__init__(self)
        self._targetURI = uri
        self._cmdargs = cmdargs
        self._content = content

    def onMessage(self, msg):
        if msg.name == 'PersistentPut':
            self.setSuccess()
            return
        miniFCP.FCPJob.onMessage(self, msg)

    def getFCPCommand(self):
        cmd = self.makeFCPCommand('ClientPut', {
            'URI': self._targetURI,
            'Verbosity': self._cmdargs.get('Verbosity', -1),
            'MaxRetries': self._cmdargs.get('MaxRetries', -1),
            'DontCompress': 'false',
            'Codecs': DEFAULT_CODEC,
            'PriorityClass': self._cmdargs.get('PriorityClass', 4),
            'Global': 'true',
            'Persistence': 'forever',
            "UploadFrom": "direct",
            "DataLength": len(self._content),
            "RealTimeFlag": "true",
        })
        transfer_mimetype_and_name(self, cmd)
        return cmd, self._content

class DDATestJob(miniFCP.FCPJob):
    def __init__(self):
        miniFCP.FCPJob.__init__(self)

    def onMessage(self, msg):
        if msg.name == 'TestDDAReply':
            self._doDDAReply(msg)
            return
        if msg.name == 'TestDDAComplete':
            self._doDDAComplete(msg)
            return
        miniFCP.FCPJob.onMessage(self, msg)

    def _startDDATest(self, directory):
        self._JobRunner._registerJob(str(directory), self)
        cmd = miniFCP.FCPCommand('TestDDARequest', {
            'Directory': directory,
            'WantReadDirectory': 'true',
            'WantWriteDirectory': 'false',
        })
        self._ConnectionRunner.sendCommand(cmd, None)

    def _doDDAReply(self, msg):
        dir = msg['Directory']
        filename = msg['ReadFilename']

        if not os.path.exists(filename):
            c = "file not found. no dda"
        else:
            f = open(filename, 'r')
            c = f.read()
            f.close()

        cmd = miniFCP.FCPCommand("TestDDAResponse", {
            'Directory': dir,
            'ReadContent': c,
        })
        self._ConnectionRunner.sendCommand(cmd, None)

    def _doDDAComplete(self, msg):
        readAllowed = msg['ReadDirectoryAllowed'] == 'true'
        self.onDDATestDone(readAllowed, False)

class PutQueueFileJob(DDATestJob):
    def __init__(self, uri, filename, **cmdargs):
        DDATestJob.__init__(self)
        self._targetURI = uri
        self._cmdargs = cmdargs
        self._filename = filename
        self._fcpcmd = None

    def onMessage(self, msg):
        if msg.name == 'PersistentPut':
            self.setSuccess()
            return
        if msg.name == 'ProtocolError':
            code = int(msg['Code'])
            if code == 9:
                self._doDirect()
            elif code == 25:
                self._startDDATest(os.path.split(self._filename)[0])
            else:
                self.setErrorMessage(msg)
            return
        DDATestJob.onMessage(self, msg)

    def getFCPCommand(self):
        if not self._fcpcmd:
            self._fcpcmd = self.makeFCPCommand('ClientPut', {
                'URI': self._targetURI,
                'Verbosity': self._cmdargs.get('Verbosity', -1),
                'MaxRetries': self._cmdargs.get('MaxRetries', 3),
                'DontCompress': 'false',
                'Codecs': DEFAULT_CODEC,
                'PriorityClass': self._cmdargs.get('PriorityClass', 4),
                'Global': 'true',
                'Persistence': 'forever',
                "UploadFrom": "disk",
                "Filename": self._filename,
                "RealTimeFlag": "true",
            })
            transfer_mimetype_and_name(self, self._fcpcmd)
        return self._fcpcmd, None

    def _doAgain(self):
        self._ConnectionRunner.sendCommand(self._fcpcmd, None)

    def _doDirect(self):
        # TODO stream the file
        f = open(self._filename, 'r')
        c = f.read()
        f.close()
        self._fcpcmd.items.update({
            "UploadFrom": "direct",
            "DataLength": len(c),
            "TargetFilename": self._cmdargs.get('TargetFilename',
                                                os.path.basename(self._filename)),
        })
        self._ConnectionRunner.sendCommand(self._fcpcmd, c)

    def onDDATestDone(self, readAllowed, writeAllowed):
        if readAllowed:
            self._doAgain()
        else:
            self._doDirect()

class FCPNode(miniFCP.FCPJobRunner):
    """High level FCP API class. Does everything on a single connection"""

    def __init__(self, **fcpargs):
        miniFCP.FCPJobRunner.__init__(self)
        self._fcpargs = fcpargs
        if __debug__:
            #hack: force fcp logging. useful at this early stage
            self._fcpargs['fcplogger'] = miniFCP.log_stdout()
        self._defaultConnectionRunner = None
        self._lastWatchGlobal = {
            'Enabled': False,
            'VerbosityMask': 0,
        }

    def _getDefaultConnectionRunner(self):
        if not self._defaultConnectionRunner:
            self._defaultConnectionRunner = miniFCP.FCPConnectionRunner(self, **self._fcpargs)
            self._defaultConnectionRunner.start()
        return self._defaultConnectionRunner

    def getConnectionRunner(self, job):
        return self._getDefaultConnectionRunner()

    def setFCPLogger(self, log=None):
        self.log = log

    def onUnhandledMessage(self, msg):
        #print "Got a msg not assigned to any job"
        #print msg
        pass

    def watchGlobal(self, Enabled=False, VerbosityMask=0):
        # TODO check for previuos set and change only if needed
        cmd = miniFCP.FCPCommand('WatchGlobal')
        if Enabled:
            cmd.items['Enabled'] = 'true'
            cmd.items['VerbosityMask'] = VerbosityMask
        self._getDefaultConnectionRunner().sendCommand(cmd)

    def getConfig(self, WithCurrent=False,WithExpertFlag=False):
        job = GetConfigJob(WithCurrent, WithExpertFlag)
        self.runJob(job)

        if job.isSuccess():
            return True, job.getConfig(), None
        else:
            return False, job._lastError, job._lastErrorMessage

    def ping(self):
        conn = self._getDefaultConnection()
        cmd = miniFCP.FCPCommand('Void')
        conn.sendCommand(cmd)
        # no reply

    def putDirect(self, uri, content, callback, **kw):
        """transient direct insert"""
        job = PutDirectJob(uri, content, callback, **kw)
        self.runJob(job)

        if job.isSuccess():
            return True, None, None
        else:
            return False, job._lastError, job._lastErrorMessage

    def putQueueData(self, uri, data, **kw):
        self.watchGlobal(True, 1)
        job = PutQueueDirectJob(uri, data, **kw)
        self.runJob(job)
        if job.isSuccess():
            return True, None, None
        else:
            return False, job._lastError, job._lastErrorMessage

    def putQueueFile(self, uri, filename, **kw):
        self.watchGlobal(True, 1)
        job = PutQueueFileJob(uri, filename, **kw)
        self.runJob(job)
        if job.isSuccess():
            return True, None, None
        else:
            return False, job._lastError, job._lastErrorMessage

