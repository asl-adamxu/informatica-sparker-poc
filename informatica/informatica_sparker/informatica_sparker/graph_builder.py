import networkx as nx
from typing import List, Dict, Set
from .models import MappingDefinition, Instance, Connector, FlowStep


class GraphBuilder:

    def __init__(self, mapping: MappingDefinition):
        self.mapping = mapping
        self.graph = nx.DiGraph()
        self.instance_map: Dict[str, Instance] = {}
        self.connected_instances: Set[str] = set()

    def build(self) -> nx.DiGraph:
        for instance in self.mapping.instances:
            self.instance_map[instance.name] = instance
            self.graph.add_node(instance.name,
                               type=instance.type,
                               transformation_type=instance.transformation_type)

        for connector in self.mapping.connectors:
            from_inst = connector.from_instance
            to_inst = connector.to_instance

            if from_inst and to_inst and from_inst != to_inst:
                self.graph.add_edge(from_inst, to_inst)
                self.connected_instances.add(from_inst)
                self.connected_instances.add(to_inst)

        return self.graph

    def get_topological_order(self) -> List[str]:
        try:
            return list(nx.topological_sort(self.graph))
        except nx.NetworkXUnfeasible:
            return list(self.graph.nodes())

    def get_ordered_flow(self) -> List[FlowStep]:
        order = self.get_topological_order()
        flow_steps = []

        for idx, instance_name in enumerate(order):
            instance = self.instance_map.get(instance_name)
            if instance:
                step = FlowStep(
                    instance_name=instance_name,
                    instance_type=instance.type or instance.transformation_type,
                    order=idx
                )
                flow_steps.append(step)

        return flow_steps

    def get_main_chain(self) -> List[str]:
        sources = self._find_sources()
        targets = self._find_targets()

        if not sources or not targets:
            return self.get_topological_order()

        longest_path = []
        for source in sources:
            for target in targets:
                try:
                    paths = list(nx.all_simple_paths(self.graph, source, target))
                    for path in paths:
                        if len(path) > len(longest_path):
                            longest_path = path
                except nx.NetworkXNoPath:
                    continue

        return longest_path if longest_path else self.get_topological_order()

    def get_detached_instances(self) -> List[str]:
        all_instances = set(self.instance_map.keys())
        return list(all_instances - self.connected_instances)

    def get_flow_text(self) -> str:
        main_chain = self.get_main_chain()

        if not main_chain:
            return "No flow detected"

        flow_parts = []
        for inst_name in main_chain:
            instance = self.instance_map.get(inst_name)
            if instance:
                inst_type = instance.type or instance.transformation_type
                short_type = self._get_short_type(inst_type)
                flow_parts.append(f"{inst_name} ({short_type})")

        return " -> ".join(flow_parts)

    def _find_sources(self) -> List[str]:
        sources = []
        for node in self.graph.nodes():
            if self.graph.in_degree(node) == 0:
                sources.append(node)
        return sources

    def _find_targets(self) -> List[str]:
        targets = []
        for node in self.graph.nodes():
            if self.graph.out_degree(node) == 0:
                targets.append(node)
        return targets

    def _get_short_type(self, type_str: str) -> str:
        type_map = {
            "Source Qualifier": "SQ",
            "Expression": "EXP",
            "Filter": "FIL",
            "Lookup Procedure": "LKP",
            "Stored Procedure": "SP",
            "Update Strategy": "UPD",
            "Aggregator": "AGG",
            "Joiner": "JNR",
            "Router": "RTR",
            "Sorter": "SRT",
            "Union": "UN",
            "Sequence Generator": "SEQ",
            "Normalizer": "NRM",
            "SOURCE": "SRC",
            "TARGET": "TGT"
        }
        return type_map.get(type_str, type_str[:3].upper() if type_str else "?")
