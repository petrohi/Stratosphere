// Copyright (c) 2010 7Clouds

using System.Threading;
using System.Collections.Generic;
using System;

namespace Stratosphere.Worker
{
    public sealed class WorkerInstance : IDisposable
    {
        public WorkerInstance(string name)
        {
            _name = name;

            try
            {
                _disposingEvent = EventWaitHandle.OpenExisting(DisposingEventName);
            }
            catch (WaitHandleCannotBeOpenedException) { }

            if (_disposingEvent == null)
            {
                _isRootInstance = true;
                _disposingEvent = new EventWaitHandle(false, EventResetMode.ManualReset, DisposingEventName);
            }
        }

        public void Dispose()
        {
            _disposingEvent.Close();
        }

        public bool IsRootInstance { get { return _isRootInstance; } }

        public void AddWorker(IWorker worker)
        {
            if (_isRootInstance)
            {
                Thread workerThread = new Thread(RunWorker);
                workerThread.Start(worker);
                
                _workerThreads.Add(workerThread);
            }
        }

        public void Run()
        {
            foreach (Thread workerThread in _workerThreads)
            {
                workerThread.Join();
            }
        }

        public void Stop()
        {
            _disposingEvent.Set();
        }

        private void RunWorker(object workerObject)
        {
            IWorker worker = (IWorker)workerObject;

            if (worker != null)
            {
                worker.Run(_disposingEvent);
            }
        }

        private string DisposingEventName { get { return string.Format("WorkerDisposing#{0}", _name); } }

        private readonly string _name;
        
        private readonly EventWaitHandle _disposingEvent;
        private readonly bool _isRootInstance;

        private readonly List<Thread> _workerThreads = new List<Thread>();
    }
}