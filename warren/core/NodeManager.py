from PyQt4.QtCore import QThread, SIGNAL, QString, pyqtSignal
from PyQt4.QtGui import QDialog, QClipboard, qApp
from warren.fcp.hyperocha import FCPNode
from warren.fcp.miniFCP import FCPConnectionRefused, FCPException
from warren.ui.FileSent import Ui_fileDroppedDialog
from warren.ui.PasteInsert import Ui_PasteInsertDialog
import FileManager
import os.path

if __debug__:
    import traceback

from pygments import highlight
from pygments import lexers
from pygments.formatters import HtmlFormatter

SECLEVELS = {'LOW':0, 'NORMAL':1, 'HIGH':2, 'MAXIMUM':3}

WARREN_DEFAULT_WATCHDOG_STARTUP_DELAY = 10000
WARREN_DEFAULT_WATCHDOG_DELAY = 1000
WARREN_DEFAULT_TIMEOUT = 5000

class NodeManager(QThread):

    pasteCanceledMessage = pyqtSignal()

    def __init__(self,config):
        QThread.__init__(self, None)
        self.config = config
        self.node = None
        self.standby = True
        self.physicalSeclevel = None
        self.nodeDownloadDir = None
        self.downloadDDA = False
        self.start()

    def run(self):
        QThread.msleep(1000) # wait a second or sometimes signals can't get through right after startup
        self.watchdog = NodeWatchdog(self)
        self.connectNode()
        self.connect(self.watchdog, SIGNAL("nodeNotConnected()"), self.nodeNotConnected)

    def connectNode(self):
        try:
            self.node = FCPNode(fcpname="WarrenClient",fcphost=self.config['node']['host'],fcpport=int(self.config['node']['fcp_port']))
            self.updateNodeConfigValues()
            self.emit(SIGNAL("nodeConnected()"))
        except FCPConnectionRefused, e:
            if __debug__:
                traceback.print_exc()
            # TODO tell the user that host/port is wrong (no TCP connect)
            self.node = None
        except FCPException, e:
            if __debug__:
                traceback.print_exc()
            # TODO tell the user that host/port can be connected, but something
            # else is wrong. Maybe not a FCP 2.0 server?
            self.node = None
        except Exception, e:
            if __debug__:
                print "somthing unexpected went wrong. BUG?"
                traceback.print_exc()
            # TODO tell the user that something unexpected went wrong. Bug?
            self.node = None

    def updateNodeConfigValues(self):
        isOK, nconfig, errmsg = self.node.getConfig(WithCurrent=True,WithExpertFlag=True)
        if isOK:
            self.physicalSeclevel = SECLEVELS[nconfig['current.security-levels.physicalThreatLevel']]
            self.nodeDownloadDir = nconfig['current.node.downloadsDir']
            self.downloadDDA = nconfig['expertFlag.fcp.assumeDownloadDDAIsAllowed']=='true'
            if not os.path.isabs(self.nodeDownloadDir):
                self.nodeDownloadDir = os.path.join(nconfig['current.node.cfgDir'], self.nodeDownloadDir)
        else:
            # getconfig failed. One of the booth values is set:
            # nconfig - the exception causing failure or None (Exception)
            # errmsg - the failure message from node or None (miniFCP.FCPMessage)
            if nconfig:
                raise nconfing
            else:
                raise Exception(str(errmsg))

    def nodeNotConnected(self):
        self.emit(SIGNAL("nodeConnectionLost()"))
        #if self.node:
            #self.node.shutdown()
        #    self.node = None
        #self.connectNode()

    def putKeyOnQueue(self, key):
        if self.physicalSeclevel > 0:
            testDDAResult = False
        else:
            if self.downloadDDA:
                testDDAResult = True
            else:
                testDDAResult = False
                try:
                    testDDA = self.node.testDDA(async=False, Directory=self.nodeDownloadDir, WantWriteDirectory=True, timeout=5)

                    if 'TestDDAComplete' in str(testDDA.items()) and "'WriteDirectoryAllowed', 'true'" in str(testDDA.items()): #TODO check for the real keys
                        testDDAResult = True
                except Exception, e:
                    testDDAResult = False

        if testDDAResult:
            filename = os.path.join(self.nodeDownloadDir, key.split('/')[-1])
            self.node.get(key,async=True, Global=True, persistence='forever',priority=4, id='Warren:'+key.split('/')[-1], file=filename)
        else:
            self.node.get(key,async=True, Global=True, persistence='forever',priority=4, id='Warren:'+key.split('/')[-1])

    def pasteCanceled(self):
        if hasattr(self, 'pasteInsert'):
            # TODO cancel request in node, too (FCP message "RemoveRequest")
            self.pasteCanceledMessage.emit()
            self.pasteInsertDialog.close()

    def newPaste(self,qPaste,lexer,lineNos):
        #TODO handle node disconnect during insert

        self.pasteInsertDialog = PasteInsert()
        self.pasteInsertDialog.show()

        self.pasteInsert = PutPaste(qPaste, lexer, lineNos, self)
        self.pasteInsert.message.connect(self.pasteInsertDialog.messageReceived)

        self.pasteInsertDialog.ui.buttonBox.rejected.connect(self.pasteCanceled)

        self.pasteInsert.start()

    def pasteMessageForwarder(self, msg):
        self.emit(SIGNAL("inserterMessage(QString)"),QString(msg))

    def insertFile(self, url, mimeType):
        if mimeType == 'directory':
            fileInsert = FileManager.DirectoryInsert(self, url)
        else:
            fileInsert = FileManager.FileInsert(self, url, mimeType, proxy=self.config['proxy']['http'])
        fileInsert.start()
        showTip = self.config['warren'].as_bool('show_file_dropped_dialog')
        if showTip:
            self.dropped = FileDropped(self)
            self.dropped.show()

    def stop(self):
        self.watchdog.quit()
        if self.node:
            pass #self.node.shutdown()
        self.quit()

class PasteInsert(QDialog):

    pasteFinished = pyqtSignal()

    def __init__(self):
        QDialog.__init__(self, None)
        self.ui = Ui_PasteInsertDialog()
        self.ui.setupUi(self)
        self.ui.progressBar.setValue(0)
        self.ui.pushButton.setDisabled(True)
        self.ui.keyLineEdit.setReadOnly(True)
        self.ui.keyLineEdit.setText('Key not yet generated... Please wait.')
        self.ui.buttonBox.buttons()[0].setDisabled(True)
        self.ui.pushButton.pressed.connect(self.pasteClipCopy)
        self.key = None

    def pasteClipCopy(self):
        clip = qApp.clipboard()
        clip.setText(str(self.key))
        if clip.supportsSelection():
            clip.setText(str(self.key),QClipboard.Selection)

    def messageReceived(self, msg):
        if msg.name == 'URIGenerated':
            self.ui.keyLineEdit.setText(msg['URI'])
            self.ui.keyLineEdit.setCursorPosition(0)
            self.ui.pushButton.setEnabled(True)
            self.key = msg['URI']
        elif msg.name == 'SimpleProgress':
            self.ui.progressBar.setMaximum(int(msg['Total']))
            self.ui.progressBar.setValue(int(msg['Succeeded']))
        elif msg.name == 'PutFailed':
            self.ui.keyLineEdit.setText('Insert Failed: '+ str(msg.items.get('CodeDescription','Unknown error')))
        elif msg.name == 'PutSuccessful':
            self.ui.buttonBox.buttons()[0].setEnabled(True)
            self.ui.buttonBox.buttons()[1].setEnabled(False)
            self.ui.keyLineEdit.setCursorPosition(0)
            self.pasteFinished.emit()
        elif msg.name == 'FinishedCompression':
            # TODO compression done
            pass
        elif msg.name == 'PutFetchable':
            # TODO it may be ok to canel the insert, it should be still fetchable
            # tell the user
            pass
        else:
            print "unhandled message "+val1

class FileDropped(QDialog):

    def __init__(self, nodeManager):
        QDialog.__init__(self, None)
        self.config = nodeManager.config
        self.ui = Ui_fileDroppedDialog()
        self.ui.setupUi(self)
        self.ui.buttonBox.accepted.connect(self.accept)

    def accept(self):
        if self.ui.checkBox.isChecked():
            self.config['warren']['show_file_dropped_dialog']=False
            self.config.write()
        self.close()

    def reject(self):
        self.hide()
        self.close()

class PutPaste(QThread):
    """ use own thread because we can't send QT signals
        asynchronously from the pyFreenet thread anyway"""

    message = pyqtSignal(object)

    def __init__(self, paste, lexer, lineNos, parent = None):
        QThread.__init__(self, parent)
        self.paste = paste
        self.nodeManager = parent
        self.node = parent.node
        self.lexer = lexer
        self.lineNos = lineNos == 'True' and 'Table' or False

    def run(self):
        keyType = self.nodeManager.config['warren']['pastebin_keytype']
        self.putPaste(self.paste, self, async=True, keyType=keyType)

    def putPaste(self, qPaste, callback, async=True, keyType='SSK@'):
        paste = unicode(qPaste)
        if self.lexer == 'text' and not self.lineNos:
            paste = paste.encode('utf-8')
            mimeType = "text/plain; charset=utf-8"
        else:
            paste = highlight(paste, lexers.get_lexer_by_name(self.lexer), HtmlFormatter(encoding='utf-8',full=True,linenos=self.lineNos))
            mimeType = "text/html; charset=utf-8"

        self.node.putDirect(keyType,paste,callback,TargetFilename='pastebin',Verbosity=5,Mimetype=mimeType,PriorityClass=2)

    def insertcb(self,msg):
        self.message.emit(msg)

    def onSimpleProgress(self, msg):
        self.insertcb(msg)

    def onURIGenerated(self, msg):
        self.insertcb(msg)

    def onSuccess(self, msg):
        self.insertcb(msg)

    def onFailure(self, msg):
        self.insertcb(msg)

    def onFinishedCompression(self, msg):
        self.insertcb(msg)

    def onPutFetchable(self, msg):
        self.insertcb(msg)

class NodeWatchdog(QThread):

    _ticks = 0

    def __init__(self,nodeManager):
        QThread.__init__(self, None)
        self.nodeManager = nodeManager
        self.start()

    def run(self):
        QThread.msleep(WARREN_DEFAULT_WATCHDOG_STARTUP_DELAY) # on startup wait additional 10 seconds
        while(True):
            _ticks =+ WARREN_DEFAULT_WATCHDOG_DELAY
            QThread.msleep(WARREN_DEFAULT_WATCHDOG_DELAY)
            if self._ticks < WARREN_DEFAULT_TIMEOUT:
                continue
            try:
                self.nodeManager.node.ping('warren_ping')
                isOK=True
            except:
                isOK=False

            if not isOK:
                self.emit(SIGNAL("nodeNotConnected()"))

    def reset(self):
        _ticks = 0
