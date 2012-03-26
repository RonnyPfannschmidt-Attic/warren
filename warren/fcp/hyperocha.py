import miniFCP
import os

DEFAULT_CODEC = 'LZMA_NEW'

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
        cmd = self.makeFCPCommand('GetConfig')
        cmd.setItem("WithCurrent", self.WithCurrent)
        cmd.setItem("WithExpertFlag", self.WithExpertFlag)
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
        cmd = self.makeFCPCommand('ClientPut')
        cmd.setItem('URI', self._targetURI)
        cmd.setItem('Verbosity', self._cmdargs.get('Verbosity', -1))
        cmd.setItem('MaxRetries', self._cmdargs.get('MaxRetries', 3))
        cmd.setItem('DontCompress', 'false')
        cmd.setItem('Codecs', DEFAULT_CODEC)
        cmd.setItem('PriorityClass', self._cmdargs.get('PriorityClass', 1))
        cmd.setItem('Global', 'false')
        cmd.setItem('Persistence', 'connection')
        cmd.setItem("UploadFrom", "direct")
        cmd.setItem("DataLength", str(len(self._content)))
        if self._cmdargs.has_key('Mimetype'):
            cmd.setItem("Metadata.ContentType", self._cmdargs['Mimetype'])
        cmd.setItem("RealTimeFlag", "true")
        if self._cmdargs.has_key('TargetFilename'):
            cmd.setItem("TargetFilename", self._cmdargs['TargetFilename'])
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
        cmd = self.makeFCPCommand('ClientPut')
        cmd.setItem('URI', self._targetURI)
        cmd.setItem('Verbosity', self._cmdargs.get('Verbosity', -1))
        cmd.setItem('MaxRetries', self._cmdargs.get('MaxRetries', -1))
        cmd.setItem('DontCompress', 'false')
        cmd.setItem('Codecs', DEFAULT_CODEC)
        cmd.setItem('PriorityClass', self._cmdargs.get('PriorityClass', 4))
        cmd.setItem('Global', 'true')
        cmd.setItem('Persistence', 'forever')
        cmd.setItem("UploadFrom", "direct")
        cmd.setItem("DataLength", str(len(self._content)))
        if self._cmdargs.has_key('Mimetype'):
            cmd.setItem("Metadata.ContentType", self._cmdargs['Mimetype'])
        cmd.setItem("RealTimeFlag", "true")
        if self._cmdargs.has_key('TargetFilename'):
            cmd.setItem("TargetFilename", self._cmdargs['TargetFilename'])
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
        cmd = miniFCP.FCPCommand('TestDDARequest')
        cmd.setItem('Directory', directory)
        cmd.setItem('WantReadDirectory', 'true')
        cmd.setItem('WantWriteDirectory', 'false')
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

        cmd = miniFCP.FCPCommand("TestDDAResponse")
        cmd.setItem('Directory', dir)
        cmd.setItem('ReadContent', c)
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
            self._fcpcmd = self.makeFCPCommand('ClientPut')
            self._fcpcmd.setItem('URI', self._targetURI)
            self._fcpcmd.setItem('Verbosity', self._cmdargs.get('Verbosity', -1))
            self._fcpcmd.setItem('MaxRetries', self._cmdargs.get('MaxRetries', 3))
            self._fcpcmd.setItem('DontCompress', 'false')
            self._fcpcmd.setItem('Codecs', DEFAULT_CODEC)
            self._fcpcmd.setItem('PriorityClass', self._cmdargs.get('PriorityClass', 4))
            self._fcpcmd.setItem('Global', 'true')
            self._fcpcmd.setItem('Persistence', 'forever')
            self._fcpcmd.setItem("UploadFrom", "disk")
            self._fcpcmd.setItem("Filename", self._filename)
            if self._cmdargs.has_key('Mimetype'):
                self._fcpcmd.setItem("Metadata.ContentType", self._cmdargs['Mimetype'])
            if self._cmdargs.has_key('TargetFilename'):
                self._fcpcmd.setItem("TargetFilename", self._cmdargs['TargetFilename'])
            self._fcpcmd.setItem("RealTimeFlag", "true")
        return self._fcpcmd, None

    def _doAgain(self):
        self._ConnectionRunner.sendCommand(self._fcpcmd, None)

    def _doDirect(self):
        # TODO stream the file
        f = open(self._filename, 'r')
        c = f.read()
        f.close()
        self._fcpcmd.setItem("UploadFrom", "direct")
        self._fcpcmd.setItem("DataLength", str(len(c)))
        self._fcpcmd.setItem("TargetFilename", self._cmdargs.get('TargetFilename', os.path.basename(self._filename)))
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
            self._fcpargs['fcplogger'] = miniFCP.FCPLogger()
        self._defaultConnectionRunner = None
        self._lastWatchGlobal = {}
        self._lastWatchGlobal['Enabled'] = False
        self._lastWatchGlobal['VerbosityMask'] = 0

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
            cmd.setItem('Enabled', 'true')
            cmd.setItem('VerbosityMask', VerbosityMask)
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

