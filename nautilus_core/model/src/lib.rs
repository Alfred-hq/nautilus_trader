// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2024 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

pub mod currencies;
pub mod data;
pub mod enums;
pub mod events;
pub mod identifiers;
pub mod instruments;
pub mod macros;
pub mod orderbook;
pub mod orders;
pub mod polymorphism;
pub mod position;
pub mod types;
pub mod venues;

#[cfg(feature = "ffi")]
pub mod ffi;
#[cfg(feature = "python")]
pub mod python;
#[cfg(feature = "stubs")]
pub mod stubs;
