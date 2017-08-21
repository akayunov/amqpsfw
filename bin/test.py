import os.path
import sys

sys.path = [os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'lib')] + sys.path

from amqpsfw.client import application
from amqpsfw import amqp_spec, ioloop

e1 = amqp_spec.Exchange.Declare('tratata')
e2 = amqp_spec.Exchange.Declare('ezezezee')
ch = amqp_spec.Channel.Open(1)
import pdb;pdb.set_trace()
print(e1)
