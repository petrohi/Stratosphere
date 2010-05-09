﻿// Copyright (c) 2010 7Clouds

using System.Collections.Generic;
using System;

namespace Stratosphere.Block
{
    public interface IContainer
    {
        string Name { get; }
        DateTime CreationDate { get; }

        IEnumerable<IBlock> ListBlocks();
        IBlock GetBlock(string name);

        void Delete();
    }
}