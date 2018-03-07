using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Comparer.Models
{
    public interface ICounter
    {
        int Value { get; }
    }
    public class RandomCounter : ICounter
    {
        static Random Rnd = new Random();
        private int _value;
        public RandomCounter()
        {
            _value = Rnd.Next(0, 1000000);
        }
        public int Value
        {
            get { return _value; }
        }
    }
    public class CounterService
    {
        protected internal ICounter Counter { get; }
        public CounterService(ICounter counter)
        {
            Counter = counter;
        }
    }
}
