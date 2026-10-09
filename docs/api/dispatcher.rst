Dispatcher and members
======================

.. autoclass:: kbus.Dispatcher
   :members: listen, close, on_message, on_disconnect, attach_link, attach_acceptor

.. autoclass:: kbus.Member
   :members: name, dispatcher, connect, close, call, send, open, route, on_reply

.. autofunction:: kbus.expose
