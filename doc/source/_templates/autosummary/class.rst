:orphan:

{{ objname | escape | underline }}

.. currentmodule:: {{ module }}

.. autoclass:: {{ objname }}
   :exclude-members: {% for item in attributes %}{{ item }}{% if not loop.last %}, {% endif %}{% endfor %}

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
