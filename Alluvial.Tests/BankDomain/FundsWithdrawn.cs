﻿using System;

namespace Alluvial.Tests.BankDomain
{
    public class FundsWithdrawn : IDomainEvent
    {
        public decimal Amount { get; set; }
        public string AggregateId { get; set; }
        public int StreamRevision { get; set; }
        public string CheckpointToken { get; set; }
    }
}