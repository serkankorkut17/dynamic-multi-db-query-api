using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DynamicDbQueryApi.DTOs
{
    public class ExpressionModel
    {
        public string Expression1 { get; set; } = "";
        public string Operation { get; set; } = "";
        public string Expression2 { get; set; } = "";

        public ExpressionModel? LeftNode { get; set; }
        public ExpressionModel? RightNode { get; set; }

        public bool IsLeaf => string.IsNullOrEmpty(Operation);
    }

}