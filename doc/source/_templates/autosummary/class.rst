:orphan:

{{ objname | escape | underline }}

.. currentmodule:: {{ module }}

.. autoclass:: {{ objname }}

   {% block attributes %}
   {% if attributes %}
   .. rubric:: Attributes
   
   {% for item in attributes %}
   .. autoattribute:: {{ item }}
      :noindex:
   {%- endfor %}
   {% endif %}
   {% endblock attributes %}

   {% block methods %}
   {% if methods %}
   .. rubric:: Methods

   {% for item in methods %}
   .. automethod:: {{ item }}
      :noindex:
   {%- endfor %}
   {% endif %}
   {% endblock methods %}
