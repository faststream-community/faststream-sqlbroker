1. Ensure parameter descriptions in SqlBrokerRegistrator.subscriber and SqlBrokerRoute.__init__ docstrings are in sync with the ones in tutorial.md (#### Subscriber parameters and #### Batch-only subscriber parameters) except that they should not mention defaults.
2. Run tests with -n auto, but be prepared for flakiness.
3. Use instructions in AGENTS.local.md if present.
4. Do not use single letter or abbreviated names unless in common exceptions.
5. Do not add docstrings or comments except for the tricky bits.
