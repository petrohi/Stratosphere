// Copyright (c) 2010, 7Clouds. All rights reserved.

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
    }
}